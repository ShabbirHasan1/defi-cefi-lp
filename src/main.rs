#[macro_use]
extern crate lazy_static;

use exlib::utils::now_ms;
use strategy::{config::*, signal::*};
// use anyhow::anyhow;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;

lazy_static! {
    pub static ref INIT_TIME: i64 = now_ms();
    pub static ref GLOABL_SIGNAL_MAP: Arc<RwLock<HashMap<String, mpsc::Sender<EverestStrategySignal>>>> =
        Arc::new(RwLock::new(HashMap::new()));
    pub static ref GIT_TS: Option<i64> =
        option_env!("GIT_TS").map(|d| (now_ms() / 1000) - d.parse::<i64>().unwrap());
}
use std::env;
#[tokio::main]
async fn main() {
    env_logger::builder().format_timestamp_millis().init();

    log::info!("current everest version: {}", env!("GIT_HASH"));
    // 读取config.json中的策略配置
    let main_config = parse_config();
    // 为每个策略分配一个task
    let mut tasks = Vec::with_capacity(1);
    // 初始化监听策略的事件
    let mut global_signal_map = GLOABL_SIGNAL_MAP.write();

    let mut strategy = timeout(Duration::from_secs(600), init_strategy(main_config.clone()))
        .await
        .unwrap()
        .unwrap();
    let (signal_tx, signal_rx) = mpsc::channel(100);
    global_signal_map.insert(main_config.excenter_config.instance_id.clone(), signal_tx);

    let task = tokio::spawn(async move {
        let res = strategy.run(signal_rx).await;
        log::error!("error! {:?}, send WILL EXIT signal...", res);

        let mut signal_map = GLOABL_SIGNAL_MAP.write();
        for (_, signal_tx) in signal_map.iter() {
            let _ = signal_tx.send(EverestStrategySignal::WILL_EXIT);
        }

        if !signal_map.is_empty() {
            signal_map.clear();
            tokio::time::sleep(Duration::from_secs(1)).await;
        }
    });
    tasks.push(task);
    drop(global_signal_map);

    // 启动策略任务
    let _ = futures::future::select_all(tasks).await;
}

mod config;
use config::*;
mod strategy;
use strategy::*;
