use excenter::prelude::*;
use exlib::prelude::*;
use futures::stream::{select_all, SelectAll};
use hashbrown::HashMap;
use tokio_stream::wrappers::BroadcastStream;
// use tokio::sync::broadcast;
use crate::EverestCredentialConfig;
use anyhow::Result;
use smallvec::smallvec;
use std::str::FromStr;

use super::*;
use std::fs;
use std::path::Path;

//缓存上下文信息
const CACHE_FILE_PATH: &str = "context_cache.json";
pub async fn save_context_to_file(contexts: HashMap<Asset, ContextData>) {
    let json_data = serde_json::to_string(&contexts).expect("Serialization failed");

    fs::write(CACHE_FILE_PATH, json_data).expect("Failed to write to file");
    log::info!("Contexts saved to {}", CACHE_FILE_PATH);
}

pub fn load_context_from_file() -> HashMap<String, ContextData> {
    if Path::new(CACHE_FILE_PATH).exists() {
        let json_data = fs::read_to_string(CACHE_FILE_PATH).expect("Failed to read from file");
        let contexts: HashMap<String, ContextData> =
            serde_json::from_str(&json_data).expect("Deserialization failed");
        log::info!(
            "Contexts loaded from {}  contexts:{:?}",
            CACHE_FILE_PATH,
            contexts
        );
        contexts
    } else {
        HashMap::new()
    }
}

/// 格式化警告信息
pub fn format_warning_message(message: &str, instance_id: &str) -> String {
    format!(
        r#"{}
InstanceID: {}
{}
-----------------------

"#,
        chrono::Local::now().to_rfc3339(),
        instance_id,
        message
    )
}

/// 获取clamp值
#[allow(dead_code)]
pub fn get_clamp_value<T: PartialOrd>(value: T, min: T, max: T) -> T {
    if value < min {
        min
    } else if value > max {
        max
    } else {
        value
    }
}

pub fn group_by_asset_by_currency(assets: Vec<Asset>) -> HashMap<Currency, AssetVec> {
    let mut result = HashMap::new();
    for asset in assets.into_iter() {
        let (origin_currency, _) = asset.pair.0.into_origin_currency();
        let list = result.entry(origin_currency).or_insert(smallvec![]);
        list.push(asset);
    }
    result
}

pub fn group_by_asset_by_exchange(assets: Vec<Asset>) -> HashMap<Exchange, AssetVec> {
    let mut result = HashMap::new();
    for asset in assets.into_iter() {
        let list = result.entry(asset.exchange).or_insert(smallvec![]);
        list.push(asset);
    }
    result
}

pub fn get_ak_map_from_credentials(
    credentials: &Vec<EverestCredentialConfig>,
) -> HashMap<Exchange, String> {
    let mut result = HashMap::new();
    for credential in credentials.into_iter() {
        let exchange = Exchange::from_str(&credential.exchange).unwrap();
        result.insert(exchange, credential.ak.clone());
    }
    result
}

pub fn merge_user_event_list(
    user_event_map: &mut Vec<BroadcastStream<(Asset, ExUserDataEvent)>>,
) -> SelectAll<BroadcastStream<(Asset, ExUserDataEvent)>> {
    let mut stream_list = Vec::with_capacity(user_event_map.len());

    while user_event_map.len() > 0 {
        let rx = user_event_map.remove(0);
        stream_list.push(rx);
    }
    select_all(stream_list)
}

pub fn merge_market_event(
    user_event_map: &mut HashMap<Asset, BroadcastStream<(Asset, ExMarketEventType)>>,
) -> SelectAll<BroadcastStream<(Asset, ExMarketEventType)>> {
    let mut stream_list = Vec::with_capacity(user_event_map.len());

    let assets: Vec<Asset> = user_event_map.keys().copied().collect();
    for asset in assets.into_iter() {
        let rx = user_event_map.remove(&asset).unwrap();
        stream_list.push(rx);
    }

    select_all(stream_list)
}
