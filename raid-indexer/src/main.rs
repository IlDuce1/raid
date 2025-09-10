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
    pub address: Address,
    pub token: Address,
    pub token_amount: U256,
    pub name: String,
    pub symbol: String,
}

struct SwapEvent
{
    pub is_buy: bool,
    pub address: Address,
    pub token: Address,
    pub token_amount: I256,
    pub name: String,
    pub symbol: String,
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

abigen!(
    UniswapV3Pool,
    r#"[
        function token0() view returns (address)
        function token1() view returns (address)
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

    // let address:Address = "0x0D6848e39114abE69054407452b8aaB82f8a44BA".parse()?;
    let topic_buy:H256 = "0x808fdde3ed072c1c71e9531614f79dbafd5a4e3e3c8e3eb605d847578aa9b499".parse()?;
    let topic_sell:H256 = "0x17f90346238171af1d19cb32cdbf0a10b0c9ddd9ed2928cb2a3729d6c7b3dc45".parse()?;
    let topic_swap: H256 = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67".parse()?;
    
    // let filter = Filter::new().address(address); //.topic0(vec![topic_buy, topic_sell]);

    let filter = Filter::new()
        .topic0(vec![topic_buy, topic_sell, topic_swap]);
    
    let client = Arc::new(provider.clone());

    // Subscribe to logs
    let mut stream = client.subscribe_logs(&filter).await?;
    
    println!("📡 Listening for Trading events...");
    while let Some(log) = stream.next().await
    {
        if log.topics[0] == topic_buy
        {
            match decode_transaction(provider.clone(), &log, &mut tokens_cache).await
            {
                Ok(event) =>
                {
                    println!("Not migrated!!!");
                    println!("Token Name: {:?} -> ({:?})", event.name, event.symbol);
                    println!(
                        "📤 Buy detected: from {:?} → to {:?}, value {},  [Hash {:?}]",
                        event.address,
                        event.token,
                        event.token_amount,
                        log.transaction_hash,
                    );

                    let buy_data = Buy
                    {
                        buyer: event.address,
                        value: event.token_amount.to_string(),
                        name: event.name.clone(),
                        symbol: event.symbol.clone(),
                        token_address: event.token,
                    };
                    
                    println!("💾 Saving to database");
                    process_buy(buy_data, &buys_collection, &tokens_collection).await?;
                    println!("✅ Saved to database");

                },
                Err(err) => { println!("{:?}", err); }
            }
        }
        else if log.topics[0] == topic_sell
        {
            match decode_transaction(provider.clone(), &log, &mut tokens_cache).await
            {
                Ok(event) =>
                {
                    println!("Not migrated!!!");
                    println!("Token Name: {:?} -> ({:?})", event.name, event.symbol);
                    println!(
                        "📤 Sell detected: from {:?} → to {:?}, value {},  [Hash {:?}]",
                        event.address,
                        event.token,
                        event.token_amount,
                        log.transaction_hash,
                    );
                    
                    let sell_data = Sell
                    {
                        seller: event.address,
                        value: event.token_amount.to_string(),
                        name: event.name.clone(),
                        symbol: event.symbol.clone(),
                        token_address: event.token,
                    };
                    
                    println!("💾 Saving to database");
                    process_sell(sell_data, &sells_collection, &tokens_collection, provider.clone()).await?;
                    println!("✅ Saved to database");
                },
                Err(err) => { println!("{:?}", err); }
            }

        }
        else if log.topics[0] == topic_swap
        {
            // Swap (address sender, index_topic_2 address recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)
            match decode_swap(provider.clone(), &log, &mut tokens_cache).await
            {
                Ok(event) =>
                {
                    if event.is_buy
                    {
                        println!("Migrated!!!");
                        println!(
                            "🟢 BUY | buyer: {:?}, token: {:?}, amount: {}, tx: {:?}",
                            event.address, event.token, event.token_amount.abs(), log.transaction_hash
                        );

                        let buy_data = Buy
                        {
                            buyer: event.address,
                            value: event.token_amount.abs().to_string(),
                            name: event.name.clone(),
                            symbol: event.symbol.clone(),
                            token_address: event.token,
                        };
                        
                        println!("💾 Saving to database");
                        process_buy(buy_data, &buys_collection, &tokens_collection).await?;
                        println!("✅ Saved to database");

                    }
                    else
                    {
                        println!("Migrated!!!");
                        println!(
                            "🔴 SELL | seller: {:?}, token: {:?}, amount: {}, tx: {:?}",
                            event.address, event.token, event.token_amount.abs(), log.transaction_hash
                        );

                        let sell_data = Sell
                        {
                            seller: event.address,
                            value: event.token_amount.abs().to_string(),
                            name: event.name.clone(),
                            symbol: event.symbol.clone(),
                            token_address: event.token,
                        };
                        
                        println!("💾 Saving to database");
                        process_sell(sell_data, &sells_collection, &tokens_collection, provider.clone()).await?;
                        println!("✅ Saved to database");

                    }
                },
                Err(err) => { println!("{:?}", err); }
            }
        }
    }

    
    Ok(())
}

async fn get_name_symbol(
    provider: Arc<Provider<Ws>>,
    token: Address,
) -> Result<(String, String), Box<dyn std::error::Error>>
{
    let contract = ERC20::new(token, provider);

    let name: String = contract.name().call().await?;
    let symbol: String = contract.symbol().call().await?;

    Ok((name, symbol))
}

async fn get_token_metadata(provider: Arc<Provider<Ws>>, token_address: Address, cache: &mut TokenCache) -> Result<(String, String), Box<dyn std::error::Error>>
{
    let addr_str = format!("{:?}", token_address);
    
    // first check memory cache
    if let Some(metadata) = cache.memory.lock().unwrap().get(&addr_str)
    {
        return Ok((metadata.clone().name, metadata.clone().symbol));
    }

    // if not in memory check mongodb
    if let Some(metadata) = cache.db.find_one(doc! {"address": &addr_str}, None).await?
    {
        cache.memory.lock().unwrap().insert(addr_str.clone(), metadata.clone());

        return Ok((metadata.clone().name, metadata.clone().symbol));
    }

    // if not in mongodb get from chain and save in both

    let (name, symbol) = get_name_symbol(provider.clone(), token_address).await?;

    let metadata = TokenMetadata
    {
       address: addr_str.clone(),
       name: name.clone(),
       symbol: symbol.clone(),
    };
    
    cache.db.insert_one(&metadata, None).await?;
    cache.memory.lock().unwrap().insert(addr_str.clone(), metadata.clone());

    Ok((name, symbol))
}

async fn decode_transaction(provider: Arc<Provider<Ws>>, log: &Log, tokens_cache: &mut TokenCache) -> Result<Event, Box<dyn std::error::Error>>
{
    let topics = &log.topics;
    if topics.len() != 3
    {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Unexpected topics length"
        )))
    }
    
    let address: Address = Address::from(topics[1]);
    let token: Address = Address::from(topics[2]);
    let token_amount: U256 = U256::from_big_endian(&log.data[0..32]);

    let (name, symbol) = get_token_metadata(provider.clone(), log.address, tokens_cache).await?;
    
    let event = Event
    {
        address,
        token,
        token_amount,
        name,
        symbol,
    };

    Ok(event)
}

async fn decode_swap(provider: Arc<Provider<Ws>>, log: &Log, tokens_cache: &mut TokenCache) -> Result<SwapEvent, Box<dyn std::error::Error>>
{
    let weth: Address = "0x3439153EB7AF838Ad19d56E1571FBD09333C2809".parse()?;
    let usdc: Address = "0x84A71ccD554Cc1b02749b35d22F684CC8ec987e1".parse()?;
    
    let topics = &log.topics;
    if topics.len() != 3
    {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Unexpected topics length"
        )))
    }

    let address: Address = Address::from(topics[1]);
    let (token0, token1) = get_token_tickers(provider.clone(), log.address).await?;

    let is_buy: bool;

    let token_amount: I256;
    let token: Address;
    
    // NOTE(): The magic numbers you see here are cutting the data into 32 bits to get out amount0 and amount1
    if token0 != weth && token1 == weth && token0 != usdc
    {
        let amount0: I256 = I256::from_raw(log.data[0..32].try_into()?);
        token_amount = amount0;
        token = token0;

        is_buy = amount0.is_negative();
    }
    else if token1 != weth && token0 == weth && token1 != usdc
    {
        let amount1: I256 = I256::from_raw(log.data[32..64].try_into()?);
        token_amount = amount1;
        token = token1;

        is_buy = amount1.is_negative();
    }
    else
    {
        return Err(Box::new(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Non weth pairs not yet supported"
        )))
    }

    let (name, symbol) = get_token_metadata(provider.clone(), token, tokens_cache).await?;
    
    let result =  SwapEvent
    {
        is_buy,
        address,
        token,
        token_amount,
        name,
        symbol,
    };

    Ok(result)
}

async fn get_token_tickers(provider: Arc<Provider<Ws>>, pair:Address) -> Result<(Address, Address), Box<dyn std::error::Error>>
{
    let pair = UniswapV3Pool::new(pair, provider);

    let token0 = pair.token_0().call().await?;
    let token1 = pair.token_1().call().await?;

    Ok((token0, token1))
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
