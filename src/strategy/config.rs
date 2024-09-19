use anyhow::Result;
use excenter::excenter::ExCenterConfig;
use exlib::prelude::*;
use exlib::ExCredential;
use hashbrown::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::str::FromStr;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LpConfig {
    pub db_instance_id: Option<String>,
    /// 发送通知的目标地址
    pub robot_info_host: Option<String>,
    /// 发送告警的目标地址
    pub robot_warn_host: Option<String>,
    // Defi 区块最大延迟时间（毫秒）
    pub max_block_delay: i64,
    // 对冲延迟
    pub hedge_time_delay: i64,
    // 监听的assets
    pub assets: Vec<String>,
    // 禁止买入的assets，仅卖出
    pub disabled_assets: Option<Vec<String>>,
    // 单个币种最大持仓
    pub max_pos_limit: f64,
    // 最小对冲比例
    pub min_hedge_rate: f64,
    // 最小对冲额度
    pub min_order_value: f64,
    // 最低日年化,较低则撤出流动性
    pub min_apy: f64,
    //cefi最大杠杆率
    pub cefi_leverage: f64,
}

impl LpConfig {
    pub fn new(global_config: &EverestStrategyConfig) -> Result<Self> {
        let config: LpConfig = serde_json::from_value(global_config.strategy_config.clone())?;
        Ok(config)
    }

    pub fn get_assets_from_config(&self) -> Result<Vec<Asset>> {
        let list = self.assets.clone();
        let mut result = Vec::with_capacity(list.len());

        for asset in list.iter() {
            result.push(Asset::from_str(asset)?);
        }

        Ok(result)
    }

    pub fn get_disabled_assets_from_config(&self) -> Result<HashSet<Asset>> {
        let mut result = HashSet::new();
        if let Some(list) = self.disabled_assets.clone() {
            for asset in list.iter() {
                result.insert(Asset::from_str(asset)?);
            }
        }

        Ok(result)
    }

    pub fn get_cefi_leverage(&self) -> f64 {
        self.cefi_leverage
    }
}

/// 策略配置信息
#[derive(Debug, Deserialize, Clone)]
pub struct EverestStrategyConfig {
    pub excenter_config: ExCenterConfig, // excenter 相关的配置
    pub strategy_config: Value,          // 策略内部的自定义参数，由每个策略自己定义
    pub credentials: Vec<EverestCredentialConfig>, // 交易所认证信息, 支持多账户
    pub strategy: String,                // 策略名
}

/// 交易所认证配置信息
#[derive(Debug, Deserialize, Clone)]
pub struct EverestCredentialConfig {
    pub exchange: String,
    pub ak: String, // Cefi API KEY 或 Defi 钱包地址
    pub sk: String, // Cefi API SECRET 或 Defi 钱包私钥
    pub pw: Option<String>,
    pub alias: Option<String>,
}

impl EverestCredentialConfig {
    pub fn get_excredential(&self) -> ExCredential {
        ExCredential {
            api_key: self.ak.clone(),
            secret_key: self.sk.clone(),
            password: self.pw.clone(),
            extra_data: None,
        }
    }
}
