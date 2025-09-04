use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use reqwest::{{header, Client}};
use ethers::prelude::*;
use std::sync::{Mutex, Arc};
use once_cell::sync::Lazy;
use dotenv::dotenv;
use std::str::FromStr;
use serde::{Serialize, Deserialize};
use chrono::Utc;
use mongodb::{bson::doc, Collection, options::{ClientOptions, ServerApi, ServerApiVersion}, Client as OtherClient};
use tokio::time::Duration;
use serde_json::Value;

struct Event
{
    pub from: Address,
    pub to: Address,
    pub value: U256,
    pub name: String,
    pub symbol: String,
    pub is_buy: bool,
    pub is_sell: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct Buy
{
    pub buyer: Address,
    pub value: String,
    pub name: String,
    pub symbol: String,
    pub token_address: Address,
}

#[derive(Debug, Serialize, Deserialize)]
struct Sell
{
    pub seller: Address,
    pub value: String,
    pub name: String,
    pub symbol: String,
    pub token_address: Address,
}


#[derive(Debug, Serialize, Deserialize)]
struct Token
{
    pub token_address: Address,
    pub name: String,
    pub symbol: String,
    pub holders: Vec<Address>,
    pub holder_count: usize,
    pub total_volume: String,
    pub total_volume_usd: f64,
    pub percentage_increase: f64,
    pub last_updated: i64,
}

// Dexscreener response
type DexscreenerResponse = Vec<Pair>;

#[derive(Debug, Deserialize)]
struct Pair
{
    #[serde(rename = "priceUsd")]
    price_usd: Option<String>,
    volume: Volume,
}

#[derive(Debug, Deserialize)]
struct Volume
{
    #[serde(rename = "h24")]
    h24: Option<f64>, // comes as string in JSON
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TokenMetadata
{
    pub address: String,
    pub name: String,
    pub symbol: String,
    pub pair: String,
}

struct TokenCache
{
    memory: Mutex<HashMap<String, TokenMetadata>>,
    db : Collection<TokenMetadata>,
}

#[derive(Debug, Clone, Copy)]
struct CacheOptions
{
    /// The time in minutes after which the cache should be refreshed.
    cache_refresh_time: u64,
    /// The timeout for the HTTP request in seconds.
    timeout: u64,
}

/// The structure for a cached response entry, holding the JSON data and a timestamp.
#[derive(Serialize, Deserialize)]
struct CacheEntry
{
    cached_response: Value,
    cached_time: u64,
}

/// A global, thread-safe cache using a Mutex to protect a HashMap.
/// The `once_cell::sync::Lazy` ensures the cache is initialized only once.
static CACHE_STORAGE: Lazy<Mutex<HashMap<String, String>>> = Lazy::new(|| {
    let map = HashMap::new();
    Mutex::new(map)
});

abigen!(
    ERC20,
    r#"[
        function pair() external view returns (address)
        function name() external view returns (string)
        function symbol() external view returns (string)
        function getCurveProgressBps() external view returns (uint256)
        function balanceOf(address account) external view returns (uint256)
    ]"#
);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>
{
    let _ = get_cached_eth_price().await;
    dotenv().ok();

    let mongodb_url = std::env::var("MONGODB_CONNECTION_STRING")?;

    let mut client_options =
        ClientOptions::parse(mongodb_url)
        .await?;
    
    // Set the server_api field of the client_options object to set the version of the Stable API on the client
    let server_api = ServerApi::builder().version(ServerApiVersion::V1).build();
    client_options.server_api = Some(server_api);
    // Get a handle to the cluster
    let mongo_client = OtherClient::with_options(client_options)?;
    
    let db = mongo_client.database("abstract_moonshot");
    let buys_collection: Collection<Buy> = db.collection("buys");
    let sells_collection: Collection<Sell> = db.collection("sells");
    let tokens_collection: Collection<Token> = db.collection("tokens");
    let tokens_metadata: Collection<TokenMetadata> = db.collection("tokens_metadata");

    let mut tokens_cache = TokenCache
    {
        memory: Mutex::new(HashMap::new()),
        db: tokens_metadata,
    };

    // Get the RPC URL from environment variables.
    let rpc_url = std::env::var("RPC_URL")?;
    let provider = Provider::<Ws>::connect(rpc_url).await?;
    let provider = Arc::new(provider);

    let event_sig = "Transfer(address,address,uint256)";

    let filter = Filter::new().event(event_sig);

    let mut stream = provider.subscribe_logs(&filter).await?;

    println!("📡 Listening for ERC20 events...");

    while let Some(log) = stream.next().await {
        match decode_transfer_log(provider.clone(), &log, &mut tokens_cache).await {
            Ok(event) =>
            {
                if event.is_buy
                {
                    let mut to = event.to;
                    if let Some(tx_hash) = log.transaction_hash
                    {
                        let raw: serde_json::Value = provider
                            .request("eth_getTransactionByHash", [tx_hash])
                            .await?;

                        // Extract manually
                        if let Some(from) = raw.get("from").and_then(|v| v.as_str())
                        {
                            to = H160::from_str(from)?
                        }
                    }
                    
                    println!("Token Name: {:?} -> ({:?})", event.name, event.symbol);
                    println!("Token Buy");
                    println!(
                        "📤 Transfer detected: from {:?} → to {:?}, value {} (token {:?}), [Hash {:?}]",
                        event.from,
                        to,
                        event.value,
                        log.address,
                        log.transaction_hash,
                    );

                    let buy_data = Buy
                    {
                        buyer: to,
                        value: event.value.to_string(),
                        name: event.name.clone(),
                        symbol: event.symbol.clone(),
                        token_address: log.address,
                    };

                    println!("💾 Saving to database");
                    process_buy(buy_data, &buys_collection, &tokens_collection).await?;
                    println!("✅ Saved to database");
                    
                }
                else if event.is_sell
                {
                    let mut from = event.from;
                    if let Some(tx_hash) = log.transaction_hash
                    {
                        let raw: serde_json::Value = provider
                            .request("eth_getTransactionByHash", [tx_hash])
                            .await?;

                        // Extract manually
                        if let Some(origin) = raw.get("from").and_then(|v| v.as_str())
                        {
                            from = H160::from_str(origin)?
                        }

                        println!("Token Name: {:?} -> ({:?})", event.name, event.symbol);
                        println!("Token Sell");
                        println!(
                            "📤 Transfer detected: from {:?} → to {:?}, value {} (token {:?}), [Hash {:?}]",
                            from,
                            event.to,
                            event.value,
                            log.address,
                            log.transaction_hash,
                        );

                        let sell_data = Sell
                        {
                            seller: from,
                            value: event.value.to_string(),
                            name: event.name.clone(),
                            symbol: event.symbol.clone(),
                            token_address: log.address,
                        };
                        
                        println!("💾 Saving to database");
                        process_sell(sell_data, &sells_collection, &tokens_collection, provider.clone()).await?;
                        println!("✅ Saved to database");
                    }

                }

            },
            Err(_err) => { }
        }
    }

    Ok(())
}

async fn get_pair_name_symbol(
    provider: Arc<Provider<Ws>>,
    token: Address,
) -> Result<(Address, String, String), Box<dyn std::error::Error>>
{
    let contract = ERC20::new(token, provider);

    let pair: Address = contract.pair().call().await?;
    let name: String = contract.name().call().await?;
    let symbol: String = contract.symbol().call().await?;

    Ok((pair, name, symbol))
}

async fn get_token_metadata(provider: Arc<Provider<Ws>>, token_address: Address, cache: &mut TokenCache) -> Result<(Address, String, String), Box<dyn std::error::Error>>
{
    let addr_str = format!("{:?}", token_address);
    
    // first check memory cache
    if let Some(metadata) = cache.memory.lock().unwrap().get(&addr_str)
    {
        let pair = Address::from_str(&metadata.clone().pair)?;
        return Ok((pair, metadata.clone().name, metadata.clone().symbol));
    }

    // if not in memory check mongodb
    if let Some(metadata) = cache.db.find_one(doc! {"address": &addr_str}, None).await?
    {
        cache.memory.lock().unwrap().insert(addr_str.clone(), metadata.clone());

        let pair = Address::from_str(&metadata.clone().pair)?;
    
        return Ok((pair, metadata.clone().name, metadata.clone().symbol));
    }

    // if not in mongodb get from chain and save in both

    let (pair, name, symbol) = get_pair_name_symbol(provider.clone(), token_address).await?;

    let metadata = TokenMetadata
    {
       address: addr_str.clone(),
       name: name.clone(),
       symbol: symbol.clone(),
       pair: pair.to_string(),
    };
    
    cache.db.insert_one(&metadata, None).await?;
    cache.memory.lock().unwrap().insert(addr_str.clone(), metadata.clone());

    Ok((pair, name, symbol))
}

async fn get_progress(
    provider: Arc<Provider<Ws>>,
    token: Address,
) -> Result<U256, Box<dyn std::error::Error>>
{
    let contract = ERC20::new(token, provider);
    let progress: U256 = contract.get_curve_progress_bps().call().await?;
    Ok(progress)
}

async fn decode_transfer_log(provider: Arc<Provider<Ws>>, log: &Log, tokens_cache: &mut TokenCache) -> Result<Event, Box<dyn std::error::Error>>
{
    let topics = &log.topics;
    if topics.len() != 3 {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Unexpected topics length"
        )))
    }

    let from: Address = Address::from(topics[1]);
    let to: Address = Address::from(topics[2]);
    let value: U256 = U256::from_big_endian(&log.data);

    let (pair, name, symbol) = get_token_metadata(provider.clone(), log.address, tokens_cache).await?;
    let progress = get_progress(provider.clone(), log.address).await?;

    let threshold: U256 = U256::from_dec_str("10000")?;

    let is_buy =
    if progress < threshold
    {
        from == log.address
    }
    else
    {
        from == pair
    };

    let is_sell =
    if progress < threshold
    {
        to == log.address
    }
    else
    {
        to == pair
    };

    let event = Event {
        from,
        to,
        value,
        name,
        symbol,
        is_buy,
        is_sell,
    };
    Ok(event)
}


async fn process_buy(
    buy: Buy,
    buys_collection: &Collection<Buy>,
    tokens_collection: &Collection<Token>,
) -> Result<(), Box<dyn std::error::Error>>
{
    let usd_price = fetch_dexscreener_price(&format!("{:?}", buy.token_address)).await?;
    let decimals = 18;
    let denominator = 10.0f64.powi(decimals as i32);
 
    buys_collection.insert_one(&buy, None).await?;

     let filter = doc! { "token_address": format!("{:?}", buy.token_address) };
    if let Some(mut token) = tokens_collection.find_one(filter.clone(), None).await?
    {
        let old_volume = U256::from_dec_str(&token.total_volume)?;
        let new_buy_volume = U256::from_dec_str(&buy.value)?;
        let new_volume = old_volume + new_buy_volume;

         let percent_increase =
        {
            let diff = new_volume.checked_sub(old_volume).unwrap_or_default();
            let old_volume_f64 = u256_to_f64(&old_volume);
            let diff_f64 = u256_to_f64(&diff);
            (diff_f64 / old_volume_f64) * 100.0
        };

        // Add holder if not exists
        if !token.holders.contains(&buy.buyer)
        {
            token.holders.push(buy.buyer);
            token.holder_count += 1;
        }

        let new_volume_f64 = u256_to_f64(&new_volume) / denominator;
        let new_volume_usd = new_volume_f64 * usd_price;
        
        token.total_volume = new_volume.to_string();
        token.total_volume_usd = new_volume_usd;
        token.percentage_increase = percent_increase;
        token.last_updated = Utc::now().timestamp();

        tokens_collection.replace_one(filter, token, None).await?;
    }
    else
    {
        let baseline_volume = fetch_dexscreener_volume(&format!("{:?}", buy.token_address)).await?;

        let buy_volume = U256::from_dec_str(&buy.value)?;
        let buy_volume_f64 = u256_to_f64(&buy_volume);
        let new_volume = baseline_volume + buy_volume_f64;
        
        let new_volume_f64 = new_volume / denominator;
        let new_volume_usd = new_volume_f64 * usd_price;
        
        let new_token = Token
        {
            token_address: buy.token_address,
            name: buy.name.clone(),
            symbol: buy.symbol.clone(),
            holders: vec![buy.buyer],
            holder_count: 1,
            total_volume: new_volume.to_string(),
            total_volume_usd: new_volume_usd,
            percentage_increase: 0.0, 
            last_updated: Utc::now().timestamp(),
        };

        tokens_collection.insert_one(new_token, None).await?;
    }

    Ok(())
}

async fn process_sell(
    sell: Sell,
    sells_collection: &Collection<Sell>,
    tokens_collection: &Collection<Token>,
    provider: Arc<Provider<Ws>>,
) -> Result<(), Box<dyn std::error::Error>>
{
    let usd_price = fetch_dexscreener_price(&format!("{:?}", sell.token_address)).await.unwrap_or(0.0);
    let decimals = 18;
    let denominator = 10.0f64.powi(decimals as i32);

    sells_collection.insert_one(&sell, None).await?;

    // 2. Check if token already exists
    let filter = doc! { "token_address": format!("{:?}", sell.token_address) };
    if let Some(mut token) = tokens_collection.find_one(filter.clone(), None).await?
    {
        // Convert volumes
        let old_volume = U256::from_dec_str(&token.total_volume)?;
        let new_sell_volume = U256::from_dec_str(&sell.value)?;
        let new_volume = old_volume + new_sell_volume;

        // Compute % increase
        let percent_increase =
        {
            let diff = new_volume.checked_sub(old_volume).unwrap_or_default();
            let old_volume_f64 = u256_to_f64(&old_volume);
            let diff_f64 = u256_to_f64(&diff);
            (diff_f64 / old_volume_f64) * 100.0
        };


        let token_contract = ERC20::new(sell.token_address, provider);
        let balance: U256 = token_contract.balance_of(sell.seller).call().await?;

        if balance.is_zero()
        {
            // Remove address from holders
            token.holders.retain(|&x| x != sell.seller);
            token.holder_count = token.holders.len();
        }

        let new_volume_f64 = u256_to_f64(&new_volume) / denominator;
        let new_volume_usd = new_volume_f64 * usd_price;
        
        token.total_volume = new_volume.to_string();
        token.total_volume_usd = new_volume_usd;
        token.percentage_increase = percent_increase;
        token.last_updated = Utc::now().timestamp();

        tokens_collection.replace_one(filter, token, None).await?;
    }
    else
    {
        let baseline_volume = fetch_dexscreener_volume(&format!("{:?}", sell.token_address)).await?;

        let sell_volume = U256::from_dec_str(&sell.value)?;
        let sell_volume_f64 = u256_to_f64(&sell_volume);
        let new_volume = baseline_volume + sell_volume_f64;
        
        let new_volume_f64 = new_volume / denominator;
        let new_volume_usd = new_volume_f64 * usd_price;
        
        let new_token = Token
        {
            token_address: sell.token_address,
            name: sell.name.clone(),
            symbol: sell.symbol.clone(),
            holders: vec![sell.seller],
            holder_count: 1,
            total_volume: new_volume.to_string(),
            total_volume_usd: new_volume_usd,
            percentage_increase: 0.0, 
            last_updated: Utc::now().timestamp(),
        };

        tokens_collection.insert_one(new_token, None).await?;
    }

    Ok(())
}

async fn fetch_dexscreener_volume(token_address: &str) -> Result<f64, Box<dyn std::error::Error>>
{
    let url = format!(
        "https://api.dexscreener.com/token-pairs/v1/abstract/{}",
        token_address
    );

    let resp = reqwest::get(&url).await?;
    let parsed: DexscreenerResponse = resp.json::<DexscreenerResponse>().await?;

    if let Some(pair) = parsed.into_iter().next()
    {
        if let Some(vol) = pair.volume.h24
        {
            return Ok(vol);
        }
    }

    Err("Volume not found for the given token.".into())
}

async fn fetch_dexscreener_price(token_address: &str) -> Result<f64, Box<dyn std::error::Error>>
{
    let url = format!(
        "https://api.dexscreener.com/token-pairs/v1/abstract/{}",
        token_address
    );

    let resp = reqwest::get(&url).await?;
    let parsed: DexscreenerResponse = resp.json::<DexscreenerResponse>().await?;

    if let Some(pair) = parsed.into_iter().next()
    {
        if let Some(price_str) = pair.price_usd
        {
            if let Ok(price) = price_str.parse::<f64>()
            {
                return Ok(price);
            }
        }
    }

    Err("Price not found for the given token.".into())
}

pub fn u256_to_f64(n: &U256) -> f64
{
    // Convert the U256 value to a decimal string to prevent overflow.
    let s = n.to_string();

    // Attempt to parse the string into an f64.
    match f64::from_str(&s) {
        Ok(f) => f,
        Err(e) => {
            // Log the error for debugging purposes instead of panicking.
            eprintln!("Failed to convert U256 to f64: {}", e);
            0.0
        }
    }
}

/// Fetches a JSON response from a URL and caches it based on the specified options.
async fn get_cached_token_price(
    url: &str,
    cache_options: CacheOptions,
    function_name: &str,
) -> Result<Value, Box<dyn std::error::Error>>
{
    let current_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let cache_key = format!("cachedFetch:{}", url);

    // Lock the mutex to access the shared cache.
    let mut storage = CACHE_STORAGE.lock().unwrap();

    // Check if the cache entry exists and is not expired.
    if let Some(serialized_data) = storage.get(&cache_key)
    {
        if let Ok(data) = serde_json::from_str::<CacheEntry>(serialized_data)
        {
            if current_time - data.cached_time < cache_options.cache_refresh_time * 60
            {
                println!("Using cached response for {}", url);
                return Ok(data.cached_response);
            }
        }
    }

    // Cache is expired or not found, proceed with a new request.
    println!("Fetching new data from {}", url);

    // Set up the HTTP client with a timeout.
    let client = Client::builder()
        .timeout(Duration::from_secs(cache_options.timeout))
        .build()?;
    
    // Set the headers.
    let mut headers = header::HeaderMap::new();
    headers.insert(header::ACCEPT, header::HeaderValue::from_static("application/json"));
    headers.insert(header::CONTENT_TYPE, header::HeaderValue::from_static("application/json"));

    // Make the HTTP GET request.
    let response = client.get(url)
        .headers(headers)
        .send()
        .await?;

    // Check for a successful HTTP status code.
    if !response.status().is_success()
    {
        return Err(format!(
            "Fetch with cache failed within function {} with status '{}'",
            function_name,
            response.status()
        ).into());
    }

    // Deserialize the JSON response.
    let response_json: Value = response.json().await?;

    // Create a new cache entry.
    let cache_entry = CacheEntry
    {
        cached_response: response_json.clone(),
        cached_time: current_time,
    };

    // Serialize and store the new cache entry.
    let serialized_entry = serde_json::to_string(&cache_entry)?;
    storage.insert(cache_key, serialized_entry);

    Ok(response_json)
}

/// Gets the current price of Ethereum in USD from an external API.
async fn get_cached_eth_price() -> f64
{
    let url = "https://api.coingecko.com/api/v3/simple/price?ids=ethereum&vs_currencies=usd";
    let cache_options = CacheOptions {
        cache_refresh_time: 15,
        timeout: 10,
    };
    let function_name = "GetCachedETHPrice";

    match get_cached_token_price(url, cache_options, function_name).await
    {
        Ok(response) =>
        {
            // Safely navigate the JSON structure to find the price.
            if let Some(eth_data) = response["ethereum"].as_object()
            {
                if let Some(usd_price) = eth_data["usd"].as_f64()
                {
                    return usd_price;
                }
            }
            eprintln!("Failed to parse JSON response for ETH price.");
            0.0
        }
        Err(e) => {
            eprintln!("Error getting cached ETH price: {}", e);
            0.0
        }
    }
}
