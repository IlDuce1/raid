use socketioxide::{extract::SocketRef, SocketIo};
use axum::routing::get;
use tracing::info;
use tracing_subscriber::FmtSubscriber;
use mongodb::{Client, Collection, options::ChangeStreamOptions};
use serde::{Serialize, Deserialize};
use futures::StreamExt;
use dotenv::dotenv;


#[derive(Debug, Serialize, Deserialize)]
struct Token
{
    name: String,
    symbol: String,
    token_address: String,
    holders: Vec<String>,
    holder_count: usize,
    total_volume: String,
    total_volume_usd: f64,
    percentage_increase: f64,
    last_updated: i64,

}

async fn on_connect(socket: SocketRef, mongo_client: axum::extract::State<Client>)
{
    info!("socket connected: {}", socket.id);

    let db = mongo_client.database("abstract_moonshot");
    let tokens_collection: Collection<Token> = db.collection("tokens");

    tokio::spawn(async move {
        let mut change_stream = tokens_collection.watch(
            None,
            Some(ChangeStreamOptions::builder().full_document(Some(mongodb::options::FullDocumentType::UpdateLookup)).build())
        ).await.unwrap();

        while let Ok(event) = change_stream.next().await.unwrap() {
            if let Some(doc) = event.full_document
            {
                info!("Change detected in MongoDB, broadcasting to client {}", socket.id);
                
                let serialized_data = serde_json::to_string(&doc).unwrap();
                let _ = socket.emit("db_update", serialized_data);
            }
        }
    });
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>>
{
    dotenv().ok();

    tracing::subscriber::set_global_default(FmtSubscriber::default())?;

    let mongodb_url = std::env::var("MONGODB_CONNECTION_STRING")?;
    let mongo_client = Client::with_uri_str(mongodb_url).await?;
    info!("Connected to MongoDB");

    let (layer, io) = SocketIo::new_layer();
    
    let mongo_client_for_socket = mongo_client.clone();
    io.ns("/", move | socket: SocketRef | on_connect(socket, axum::extract::State(mongo_client_for_socket.clone())));
    
    let app = axum::Router::new()
        .route("/", get(|| async { "Hello Raid" }))
        .layer(layer);
    
    info!("Starting Server");

    
    let listener = tokio::net::TcpListener::bind("0.0.0.0:3000").await?;
    axum::serve(listener, app).await?;
    
    Ok(())
}

