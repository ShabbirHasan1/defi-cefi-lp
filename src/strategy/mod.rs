use self::{
    cefi::{
        get_base_coin_usdt_price, get_cefi_depth, get_cefi_pos_and_account, get_currency_price,
        get_origin_asset,
    },
    config::LpConfig,
    defi::{get_defi_depth, get_defi_order_pending_and_end, get_defi_pos_and_account},
    utils::*,
};

use super::*;
use anyhow::{anyhow, Ok, Result};
use excenter::market::EX_MARKET;
use excenter::prelude::*;
use exlib::prelude::*;
use exlib::utils::now_ms;
use futures::StreamExt;
use hashbrown::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::sleep;
use tokio_stream::wrappers::BroadcastStream;

pub struct LpStrategy {
    instance_id: String,
    db_instance_id: String,
    excenter: ExCenter,
    assets_by_exchange: HashMap<Exchange, AssetVec>,
    assets_by_currency: HashMap<Currency, AssetVec>,
    ak_map: HashMap<Exchange, String>,
    global_config: EverestStrategyConfig,
    assets: Vec<Asset>,
    disabled_assets: HashSet<Asset>, // 禁止交易的币种
    market_event_map: HashMap<Asset, BroadcastStream<(Asset, ExMarketEventType)>>,
    user_event_list: Vec<BroadcastStream<(Asset, ExUserDataEvent)>>,
    record_time_map: HashMap<Asset, i64>,
    config: LpConfig,
    last_record_time: i64,
    last_hedge_time: HashMap<Currency, i64>,
    last_hedge_signal_time: HashMap<Asset, i64>,
    defi_eth_pos: HashMap<Asset, f64>,
    latest_depth_time: HashMap<Asset, i64>, // 当前币种的最新深度时间
    latest_current_depth_time: i64,
    contexts: HashMap<Asset, ContextData>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
//统计每分钟
pub struct ContextData {
    //最近订单
    last_order_id: Option<String>,
    //最近订单成交时间
    last_order_ts: i64,
    //持仓
    lp_pos: f64,
    //持仓均价
    price_avg: f64,
    //买入时总价值
    pos_eth_wealth: f64,
    //买入时总价值
    pos_usd_wealth: f64,
    //当前总价值
    current_pos_usd_wealth: f64,
    //买入时token0投入
    token0_use: f64,
    //买入时token1投入
    token1_use: f64,
    //token0对冲仓位
    token0_hedge: f64,
    //token1对冲仓位
    token1_hedge: f64,
    //池子中的价格
    current_price: f64,
    //对冲信号产生时间
    hedge_signal_ts: i64,
}

impl LpStrategy {
    pub async fn new(global_config: EverestStrategyConfig, excenter: ExCenter) -> Result<Self> {
        let instance_id = excenter.config.instance_id.clone();
        let config: LpConfig = LpConfig::new(&global_config)?;
        let assets = config.get_assets_from_config()?;
        let disabled_assets = config.get_disabled_assets_from_config()?;
        let db_instance_id = if let Some(db_instance_id) = config.db_instance_id.clone() {
            db_instance_id
        } else {
            instance_id.clone()
        };

        excenter.send_message_to_robot(
            &format!("策略启动成功 {}", instance_id),
            "----------------------",
            config.robot_info_host.as_deref(),
        );

        let mut strategy = LpStrategy {
            ak_map: get_ak_map_from_credentials(&global_config.credentials),
            assets_by_exchange: group_by_asset_by_exchange(assets.clone()),
            assets_by_currency: group_by_asset_by_currency(assets.clone()),
            instance_id,
            db_instance_id,
            excenter,
            global_config,
            assets,
            disabled_assets,
            market_event_map: HashMap::new(),
            user_event_list: vec![],
            config,
            last_record_time: 0,
            last_hedge_signal_time: HashMap::new(),
            last_hedge_time: HashMap::new(),
            latest_depth_time: HashMap::new(),
            contexts: HashMap::new(),
            latest_current_depth_time: 0,
            defi_eth_pos: HashMap::new(),
            record_time_map: HashMap::new(),
        };
        //初始化历史数据
        let context_data = load_context_from_file();
        for asset in strategy.assets.iter() {
            let asset_str = asset.to_string();
            if let Some(v) = context_data.get(&asset_str) {
                strategy.contexts.insert(asset.clone(), v.clone());
            }
        }
        strategy.init().await?;
        Ok(strategy)
    }

    async fn init(&mut self) -> Result<()> {
        let mut config = ExClientConfig::default();
        let ak = self.ak_map.get(&Exchange::ETHEREUM).unwrap();
        if let Some(custom_map) = self
            .global_config
            .excenter_config
            .client_custom_config
            .as_ref()
        {
            if let Some(custom_config) = custom_map.get(ak) {
                config
                    .custom_config
                    .replace(serde_json::from_value(custom_config.clone())?);
            }
        }

        let market_config = ExMarketSubConfig {
            sub_depth: true,
            sub_index: None,
            sub_funding_rate: true,
            sub_kline: vec![],
            sub_trade: false,
            sub_mempool: Some(false), // 只有defi才有mempool， cefi会自动忽略这个字段
            skip_history_kline: None,
            kline_length: 0,
            trade_interval: Some(1000),
            funding_rate_hours: Some(24),
            depth_channel_size: 1,
            trade_channel_size: 1,
            enable_private: false,
            market_extra_data_length: 0,
            sub_index_kline: vec![],
            sub_market_extra_data: vec![],
            multi_source_count: self.global_config.excenter_config.multi_source_count,
            enable_depth_record: false,
            enable_trade_record: false,
            sub_liquidation: false,
            liquidation_interval: None,
            liquidation_channel_size: None,
            public_credential: None,
            public_client_config: Some(config),
            public_custom_config: None,
            enable_bkmarket: None,
        };

        if EX_MARKET.get_client(Exchange::ETHEREUM).is_err() {
            let client = self.excenter.get_client(ak).unwrap();
            EX_MARKET.add_client(Exchange::ETHEREUM, client);
        }

        for (exchange, asset_list) in self.assets_by_exchange.iter() {
            let ak = self.ak_map.get(exchange).unwrap();

            if !exchange.is_defi() {
                let client = self.excenter.get_client(&ak)?;
                // 初始化杠杆
                for asset in asset_list.iter() {
                    if asset.asset_type == AssetType::SWAP {
                        // Bybit 有时会返回错误，但实际成功，所以忽略错误
                        let rsp = client
                            .set_levereage(asset.clone(), self.config.get_cefi_leverage())
                            .await;
                        if rsp.is_err() {
                            log::error!("set_levereage failed, asset: {:?}, err: {:?}", asset, rsp);
                        } else {
                            log::info!(
                                "set_levereage successed, asset: {:?}, err: {:?}",
                                asset,
                                rsp
                            );
                        }
                    }
                }
                // 设置为单向持仓模式。有一些不支持的会返回错误，忽略
                let _rsp = client.change_pos_direction_mode(true).await;
            }

            // 订阅公共行情
            let market_event_map = EX_MARKET
                .subscribe_public_channel(asset_list.clone(), Some(market_config.clone()))
                .await?;
            for (asset, rx) in market_event_map.into_iter() {
                self.market_event_map
                    .insert(asset, BroadcastStream::new(rx));
            }
            // 订阅用户行情
            let user_event_map = self
                .excenter
                .subscribe_user_channel(ak, asset_list.clone())
                .await?;
            for (_, rx) in user_event_map.into_iter() {
                self.user_event_list.push(BroadcastStream::new(rx));
            }
        }

        Ok(())
    }

    async fn on_tick(&mut self, cefi_asset: Asset, defi_asset: Asset) -> Result<()> {
        // 获取cefi depth
        let cefi_depth = get_cefi_depth(&cefi_asset)?;
        if cefi_depth.is_none() {
            return Ok(());
        }

        let cefi_depth = cefi_depth.unwrap();
        if cefi_depth.asks.len() == 0 || cefi_depth.bids.len() == 0 {
            let msg = format!(
                "Cefi Depth 为空，{:?}, asks len {}, bids len {}",
                cefi_asset,
                cefi_depth.asks.len(),
                cefi_depth.bids.len()
            );
            self.excenter.send_message_to_robot(
                "Cefi Depth为空,策略重启",
                &format_warning_message(&msg, &self.excenter.config.instance_id),
                Some(self.config.robot_warn_host.clone().unwrap().as_str()),
            );
            return Err(anyhow!(msg));
        }
        self.check_market_is_alive(&cefi_asset, cefi_depth.time)
            .await?;

        // 获取defi depth
        let defi_depth = get_defi_depth(&defi_asset)?;
        if defi_depth.is_none() {
            return Ok(());
        }
        let defi_depth: DepthData = defi_depth.unwrap();
        self.check_market_is_alive(&defi_asset, defi_depth.time)
            .await?;
        if defi_depth.mid_price() == 0.0 {
            // defi 流动性为0
            return Ok(());
        }

        let mut field_map: HashMap<String, Value> = HashMap::new();
        let mut tag_map = HashMap::new();
        tag_map.insert("defi_asset".to_string(), defi_asset.to_string());
        tag_map.insert("cefi_asset".to_string(), cefi_asset.to_string());
        tag_map.insert("exchange".to_string(), "ETHEREUM".to_string());

        let cefi_ak = self.ak_map.get(&cefi_asset.exchange).unwrap().clone();
        let cefi_pos_account = get_cefi_pos_and_account(&self.excenter, &cefi_asset, &cefi_ak)?;

        let defi_ak = self.ak_map.get(&defi_asset.exchange).unwrap().clone();
        let defi_pos_account = get_defi_pos_and_account(&self.excenter, &defi_asset, &defi_ak)?;
        if defi_pos_account.is_none() || cefi_pos_account.is_none() {
            return Ok(());
        }
        let base_coin_price = get_currency_price(defi_asset.pair.1)?;
        let result =
            self.update_context(defi_asset, defi_ak.as_str(), base_coin_price, &defi_depth)?;
        if result {
            //缓存
            let contexts = self.contexts.clone();
            save_context_to_file(contexts).await;
        }

        let (cefi_pos, _) = cefi_pos_account.unwrap();
        //管理流动性，暂时先只考虑筛选出来的asset直接添加流动性，按底池比例，和最大仓位来添加
        {
            // let tvl: f64 = base_coin_price * defi_depth.asks[0].volume * 2.0;
            // //5% tvl
            // let max_pos_limit = (tvl * 0.05).min(self.config.max_pos_limit);
            // let order_value = max_pos_limit - context_data.pos_usd_wealth;
            // let order_value_eth = order_value / base_coin_price;
            // let has_pending = self.has_pending_order(defi_asset, &defi_ak)?;
            // log::info!(
            //     "lp_post_order  add_liquidity {}  {}  {}  {}",
            //     order_value_eth,
            //     self.config.min_order_value,
            //     has_pending,
            //     base_coin_price
            // );
            // if order_value >= self.config.min_order_value && !has_pending {
            //     self.lp_post_order(defi_asset.clone(), order_value_eth, true, 0.0)
            //         .await?;
            // }
        }

        //对冲管理
        let lp_balance = defi_depth.lp_balance.unwrap();
        let lp_total_supply = defi_depth.lp_total.unwrap();
        let reserve0 = defi_depth.bids[0].volume;
        let reserve1 = defi_depth.asks[0].volume;
        let amount_0 = reserve0 * lp_balance / lp_total_supply;
        let amount_1 = reserve1 * lp_balance / lp_total_supply;

        //记录每个币的eth仓位
        self.defi_eth_pos.insert(defi_asset, amount_1);

        field_map.insert("lp_balance".to_string(), json!(lp_balance));
        field_map.insert("lp_total_supply".to_string(), json!(lp_total_supply));
        field_map.insert("reserve0".to_string(), json!(reserve0));
        field_map.insert("reserve1".to_string(), json!(reserve1));
        field_map.insert("amount_0".to_string(), json!(amount_0));
        field_map.insert("amount_1".to_string(), json!(amount_1));

        //按照持仓的lp数量占比计算两种币的数量,进行对冲
        let hedge_amount0 = amount_0 - cefi_pos.volume.abs();
        field_map.insert("token0_hedge".to_string(), json!(cefi_pos.volume));
        field_map.insert("token1_hedge".to_string(), json!(amount_1));
        let defi_wealth_total = amount_1 * base_coin_price * 2.0;
        field_map.insert(
            "defi_current_pos_usd_wealth".to_string(),
            json!(defi_wealth_total),
        );

        let cefi_wealth_total = cefi_pos.volume * cefi_depth.mid_price() * 2.0;
        field_map.insert(
            "cefi_current_pos_usd_wealth".to_string(),
            json!(cefi_wealth_total),
        );
        field_map.insert(
            "total_wealth".to_string(),
            json!(cefi_wealth_total.abs() + defi_wealth_total),
        );

        let now_ms = now_ms();
        let signal_time = *self.last_hedge_signal_time.get(&defi_asset).unwrap_or(&0);
        let cefi_price = cefi_depth.mid_price();
        let defi_price = defi_depth.mid_price() * base_coin_price;
        field_map.insert("cefi_price".to_string(), json!(cefi_price));
        field_map.insert("defi_price".to_string(), json!(defi_price));

        //禁止对冲币种不对冲
        //eth 对冲信号出现15s后再对冲,相当于延迟一个区块对冲
        if now_ms - signal_time > self.config.hedge_time_delay
            && (hedge_amount0.abs() > cefi_pos.volume.abs() * self.config.min_hedge_rate)
            && !self.disabled_assets.contains(&defi_asset)
        {
            log::info!(
                "on_tick hedge check  :  hedge_amount0  {}  lp_balance {}  lp_total_supply {} cefi_pos.volume {}  amount_0 {}",
                hedge_amount0,
                lp_balance,
                lp_total_supply,
                cefi_pos.volume,
                amount_0
            );
            if signal_time == 0 || now_ms - signal_time > 24000 {
                self.last_hedge_signal_time.insert(defi_asset, now_ms);
                return Ok(());
            }

            //对冲token0
            self.hedging(
                &cefi_asset,
                -hedge_amount0,
                &cefi_pos,
                cefi_depth.mid_price(),
                &cefi_ak,
            )
            .await?;

            log::info!(
                "hedge_price   defi_price  {} cefi_price  {}  hedge_amount0{}",
                defi_price,
                cefi_price,
                hedge_amount0
            );
            self.last_hedge_signal_time.insert(defi_asset, now_ms);
        }

        //获取eth的仓位
        let currency = defi_asset.pair.1;
        if currency != Currency::USDT && currency != Currency::USDC {
            let (origin_currency, _) = currency.into_origin_currency();
            let assets = self.assets_by_currency.get(&origin_currency);
            if assets.is_none() {
                return Err(anyhow!("no match currency"));
            } else {
                let assets = assets.unwrap();
                let cefi_assets: Vec<Asset> = assets
                    .clone()
                    .into_iter()
                    .filter(|a| !a.exchange.is_defi() && a.asset_type == AssetType::SWAP)
                    .collect();
                if cefi_assets.len() > 0 {
                    //取第一个来对冲
                    let cefi_asset1 = cefi_assets[0];
                    let cefi_pos_account =
                        get_cefi_pos_and_account(&self.excenter, &cefi_asset1, &cefi_ak)?;
                    let (cefi_pos, _) = cefi_pos_account.unwrap();

                    let cefi_depth = get_cefi_depth(&cefi_asset1)?;
                    if cefi_depth.is_none() {
                        return Ok(());
                    }

                    let defi_asset_count = self
                        .assets
                        .iter()
                        .filter(|asset| asset.exchange.is_defi())
                        .count();
                    let token1_defi_pos: f64 = self.defi_eth_pos.iter().map(|(_, pos)| pos).sum();

                    field_map.insert("defi_eth_pos".to_string(), json!(token1_defi_pos));
                    field_map.insert("cefi_eth_pos".to_string(), json!(cefi_pos.volume));

                    //等所有币种的defi eth仓位都加载完成再检查对冲
                    if self.defi_eth_pos.len() == defi_asset_count {
                        //对冲token1,主要是eth,usd不对冲
                        let last_hedge_time = self.last_hedge_time.entry(currency).or_insert(0);
                        if now_ms - *last_hedge_time < 15000 {
                            return Ok(());
                        }

                        let hedge_amount1 = token1_defi_pos - cefi_pos.volume.abs();
                        let cefi_depth = cefi_depth.unwrap();
                        if hedge_amount1.abs() > cefi_pos.volume.abs() * self.config.min_hedge_rate
                        {
                            //对冲token1
                            self.hedging(
                                &cefi_asset1,
                                -hedge_amount1,
                                &cefi_pos,
                                cefi_depth.mid_price(),
                                &cefi_ak,
                            )
                            .await?;
                        }
                    }
                }
            }
        }

        let record_time = self.record_time_map.get(&defi_asset).unwrap_or(&0);
        //上报持仓信息手续费收入
        if now_ms - record_time > 5000 {
            self.record_custom_data(field_map, tag_map, "defi_cefi_detail");
            self.record_time_map.insert(defi_asset, now_ms);
        }

        Ok(())
    }

    /// 对冲
    pub async fn hedging(
        &mut self,
        cefi_asset: &Asset,
        mut size: f64,
        cefi_pos: &PositionData,
        cefi_price: f64,
        cefi_ak: &str,
    ) -> Result<bool> {
        let currency = cefi_asset.pair.0;
        {
            // 检查cefi是否可以对冲
            let ctx_arc = self.excenter.get_context(cefi_asset.clone(), cefi_ak)?;
            let ctx = ctx_arc.read();
            // 如果有未成交的订单，则不对冲，tick结束返回
            if !ctx.is_post_safe() || !ctx.is_open_order_safe() {
                return Ok(false);
            }
        }

        let cefi_ak = self.ak_map.get(&cefi_asset.exchange).unwrap().clone();
        // cefi 模拟仓位
        let rule = self
            .excenter
            .get_trade_rule(*cefi_asset)
            .unwrap_or(AssetTradeRule::default());
        let cefi_pos_coin_value = cefi_pos.volume * rule.face_value;

        // 检查对冲时间间隔，同一币种不能少于12s
        let now = now_ms();
        let last_hedge_time = self.last_hedge_time.entry(currency).or_insert(0);
        // TODO：使用1个区块时间12秒时，出现ETH反复对冲的情形。疑似多dex同币种，容易出现depth更新超过1个block的情形
        if now - *last_hedge_time < 15000 {
            log::warn!("{} hedging time limit", currency);
            return Ok(false);
        }
        *last_hedge_time = now;

        self.excenter.send_message_to_robot(
            &format!("Cefi对冲 {}", cefi_asset.to_string()),
            &format_warning_message(
                &format!(
                    r#"
Cefi对冲 {}

DeltaPos {}

CefiPrice {}

CefiPos {}

enable_hedge {}
"#,
                    cefi_asset.to_string(),
                    size,
                    cefi_price,
                    cefi_pos_coin_value,
                    self.config.enable_hedge,
                ),
                &self.excenter.config.instance_id,
            ),
            Some(self.config.robot_info_host.clone().unwrap().as_str()),
        );

        //禁止下单提前返回不下单
        if !self.config.enable_hedge {
            return Ok(false);
        }

        {
            // 如果是正常的币种或者乘数币种，直接对冲
            let (_, multiplier) = get_origin_asset(cefi_asset);
            if multiplier > 1.0 {
                size /= multiplier;
            }
            // 转换成合规的精度
            size = rule.get_safe_size_simple(size, true);
            // 单次对冲数量不能超过最大下单数量
            if size > 0.0 {
                size = rule.max_market_order_size.unwrap_or(size).min(size);
            } else if size < 0.0 {
                size = (-rule.max_market_order_size.unwrap_or(-size)).max(size);
            } else {
                // 数量为0，不对冲
                return Ok(false);
            }

            // 其他交易所，使用市价单对冲
            // None
            let mut req = OrderRequest::new(None, size, *cefi_asset);
            req.time_in_force = OrderTimeInForce::IOC;
            self.excenter.post_order(req, &cefi_ak, false)?;

            return Ok(true);
        }
    }

    fn update_context_hedge_data(
        &mut self,
        asset: Asset,
        token0_hedge: f64,
        token1_hedge: f64,
        signal_ts: i64,
    ) {
        let ctx = self.contexts.get_mut(&asset);
        if ctx.is_some() {
            let ctx = ctx.unwrap();
            if token0_hedge != 0.0 || token1_hedge != 0.0 {
                ctx.token0_hedge += token0_hedge;
                ctx.token1_hedge += token1_hedge;
            }
            ctx.hedge_signal_ts = signal_ts;
        }
    }

    fn update_context(
        &mut self,
        asset: Asset,
        ak: &str,
        base_coin_price: f64,
        defi_depth: &DepthData,
    ) -> Result<bool> {
        let now = now_ms();
        match self.contexts.get_mut(&asset) {
            Some(context) => {
                let res = get_defi_order_pending_and_end(&self.excenter, &asset, ak)?;
                let (_, end_orders) = res;

                context.current_price = defi_depth.mid_price();
                let lp_balance = defi_depth.lp_balance.unwrap();
                let lp_total_supply = defi_depth.lp_total.unwrap();
                //let reserve0 = defi_depth.bids[0].volume;
                let reserve1 = defi_depth.asks[0].volume;
                let amount_1 = reserve1 * lp_balance / lp_total_supply;
                context.current_pos_usd_wealth = amount_1 * 2.0 * base_coin_price;

                if end_orders.is_some() {
                    let end_orders = end_orders.unwrap();
                    let mut latest_order = None;
                    for order in end_orders {
                        let d = order.data.client_oid.clone().unwrap();
                        if d == asset.to_string() {
                            latest_order = Some(order.data);
                            break;
                        }
                    }
                    if latest_order.is_some() {
                        let latest_order = latest_order.unwrap();
                        //交易成功
                        if latest_order.status == OrderStatus::FULL_FILLED {
                            let extra_data = latest_order.extra_data.unwrap();
                            let value = extra_data.get("order_type").unwrap();
                            let order_type: u8 = serde_json::from_value(value.clone()).unwrap();
                            let value = extra_data.get("amount0").unwrap();
                            let amount0: f64 = serde_json::from_value(value.clone()).unwrap();
                            let value = extra_data.get("amount1").unwrap();
                            let amount1: f64 = serde_json::from_value(value.clone()).unwrap();
                            let value = extra_data.get("liquidity").unwrap();
                            let liquidity: f64 = serde_json::from_value(value.clone()).unwrap();
                            let value = extra_data.get("eth_amount").unwrap();
                            let eth_amount: f64 = serde_json::from_value(value.clone()).unwrap();

                            context.last_order_id = latest_order.id;
                            context.last_order_ts = latest_order.client_updated_at;
                            let eth_price = get_base_coin_usdt_price(&asset).unwrap();
                            if order_type == 1 {
                                context.lp_pos += liquidity;
                                context.token0_use += amount0;
                                context.token1_use += amount1;
                                context.pos_eth_wealth += eth_amount;
                                context.pos_usd_wealth += eth_amount * eth_price;
                                context.price_avg = context.token1_use / context.token0_use;
                            } else {
                                //撤出流动性暂时全部撤出
                                context.lp_pos = 0.0;
                                context.token0_use = 0.0;
                                context.token1_use = 0.0;
                                context.pos_eth_wealth = 0.0;
                                context.pos_usd_wealth = 0.0;
                                context.price_avg = 0.0;
                            }

                            log::info!("");

                            return Ok(true);
                        }
                    }
                }
            }
            None => {
                self.contexts.insert(
                    asset,
                    ContextData {
                        last_order_id: None,
                        last_order_ts: now,
                        lp_pos: 0.0,
                        pos_eth_wealth: 0.0,
                        pos_usd_wealth: 0.0,
                        current_pos_usd_wealth: 0.0,
                        token0_use: 0.0,
                        token1_use: 0.0,
                        price_avg: 0.0,
                        token0_hedge: 0.0,
                        token1_hedge: 0.0,
                        hedge_signal_ts: 0,
                        current_price: 0.0,
                    },
                );
            }
        }
        Ok(false)
    }

    //有pending则不交易
    fn has_pending_order(&mut self, asset: Asset, ak: &str) -> Result<bool> {
        let res = get_defi_order_pending_and_end(&self.excenter, &asset, ak)?;
        let (pending_order, _) = res;
        if pending_order.is_some() {
            return Ok(true);
        }

        Ok(false)
    }

    async fn lp_post_order(
        &mut self,
        asset: Asset,
        size: f64,
        add_liquidity: bool,
        price: f64,
    ) -> Result<bool> {
        let defi_ak = self.ak_map.get(&asset.exchange).unwrap().clone();

        let g = 1.0;
        let t = 0.0;
        let mut dex_order_req = OrderRequest::new(Some(price), size, asset.clone());
        let mut extra_data: HashMap<String, Value> = HashMap::new();
        let native_currency_price = 1.0;

        extra_data.insert("add_liquidity".to_string(), json!(add_liquidity));
        extra_data.insert("amount".to_string(), json!(size));
        extra_data.insert("eth_price".to_string(), json!(native_currency_price));
        extra_data.insert("native_price".to_string(), json!(native_currency_price));
        let pair1_price = 1.0;
        extra_data.insert("pair1_price".to_string(), json!(pair1_price));
        extra_data.insert("gas".to_string(), json!(g / native_currency_price));
        extra_data.insert("taker_fee".to_string(), json!(t));
        let tx_url_prefix = "";
        extra_data.insert("tx_url_prefix".to_string(), json!(tx_url_prefix));
        extra_data.insert(
            "exchange".to_string(),
            json!(asset.exchange.clone().to_string()),
        );
        let is_defi = if asset.exchange.is_defi() { 1 } else { 0 };
        extra_data.insert("is_defi".to_string(), json!(is_defi));
        extra_data.insert(
            "defi_swap".to_string(),
            json!(asset.get_defi_swap().unwrap().to_string()),
        );
        extra_data.insert(
            "defi_chain".to_string(),
            json!(asset.get_defi_chain().unwrap().to_string()),
        );
        extra_data.insert("quote_token".to_string(), json!(asset.pair.1.to_string()));
        dex_order_req.extra_data = Some(extra_data);
        //使用
        dex_order_req.client_oid = Some(asset.to_string());
        let res = self.excenter.post_order(dex_order_req, &defi_ak, false);
        if res.is_err() {
            log::error!("Lp_post_order failed, post order failed {:?}", asset);
            return Ok(false);
        }
        Ok(true)
    }

    // 检查当前的市场深度更新是否及时，是否存在断线情况
    async fn check_market_is_alive(&mut self, asset: &Asset, depth_time: i64) -> Result<()> {
        let now = now_ms();
        if asset.exchange.is_defi() {
            // 检查defi节点区块延迟
            let now = now_ms();
            //let defi_ak = self.ak_map.get(&asset.exchange).unwrap().clone();
            // let client = self.excenter.get_client(&defi_ak)?;
            let client = EX_MARKET.get_client(Exchange::ETHEREUM)?;
            let latest_block_time = client.get_latest_block_time().await?;
            if now - latest_block_time > self.config.max_block_delay {
                let msg: String = format!(
                    "defi block is too old! block time {} diff {}",
                    latest_block_time,
                    now - latest_block_time,
                );
                self.excenter.send_message_to_robot(
                    "Defi节点区块延迟",
                    &format_warning_message(&msg, &self.excenter.config.instance_id),
                    self.config.robot_warn_host.as_deref(),
                );
                return Err(anyhow!(msg));
            }

            // 更新defi所有asset的最新深度时间
            if depth_time > self.latest_current_depth_time {
                self.latest_current_depth_time = depth_time;
            }

            // 更新当前asset的最新深度时间
            let latest_depth_time = self.latest_depth_time.entry(asset.clone()).or_insert(0);
            if depth_time > *latest_depth_time {
                *latest_depth_time = depth_time;
            }

            // 检查Defi所有asset最新深度时间
            let depth_time = self.latest_current_depth_time;
            // 如果Defi所有币种深度时间超时 10 分钟没有更新，报错
            if now - depth_time > 120000 * 10 {
                let msg = format!(
                    "defi asset depth is too old! depth time {} diff {}",
                    depth_time,
                    now - depth_time,
                );
                self.excenter.send_message_to_robot(
                    "行情中断",
                    &format_warning_message(&msg, &self.excenter.config.instance_id),
                    self.config.robot_warn_host.as_deref(),
                );
                return Err(anyhow!(msg));
            }

            // 检查Defi asset最新深度时间
            let depth_time = *latest_depth_time;
            // 如果单asset的深度时间超时 8 小时没有更新，报警但不重启
            if now - depth_time > 60000 * 60 * 8 {
                let msg = format!(
                    "asset {} depth is too old! depth time {} diff {}",
                    asset.to_string(),
                    depth_time,
                    now - depth_time,
                );
                self.excenter.send_message_to_robot(
                    "行情8小时没有更新",
                    &format_warning_message(&msg, &self.excenter.config.instance_id),
                    self.config.robot_info_host.as_deref(),
                );
                // 重置提醒时间
                *latest_depth_time = now;
            }
        } else {
            // 如果非稳定币的深度时间超时 10 分钟，报错
            if now - depth_time > 600000 && !asset.pair.0.is_usd_based() {
                let msg = format!(
                    "asset {} depth is too old! depth time {} diff {}",
                    asset.to_string(),
                    depth_time,
                    now - depth_time,
                );
                self.excenter.send_message_to_robot(
                    "行情中断",
                    &format_warning_message(&msg, &self.excenter.config.instance_id),
                    self.config.robot_warn_host.as_deref(),
                );
                return Err(anyhow!(msg));
            }
            // 如果主流币的深度时间超时 3 分钟，报错
            let currency = asset.pair.0;
            if now - depth_time > 60000 * 3
                && (currency == Currency::BTC || currency == Currency::ETH)
            {
                let msg = format!(
                    "asset {} depth is too old! depth time {} diff {}",
                    asset.to_string(),
                    depth_time,
                    now - depth_time,
                );
                self.excenter.send_message_to_robot(
                    "行情中断",
                    &format_warning_message(&msg, &self.excenter.config.instance_id),
                    self.config.robot_warn_host.as_deref(),
                );
                return Err(anyhow!(msg));
            }
        }

        Ok(())
    }

    fn record_custom_data(
        &self,
        field_map: HashMap<String, Value>,
        tag_map: HashMap<String, String>,
        table: &str,
    ) {
        EX_DATA_REPORTER.record_custom_data(
            table,
            field_map,
            tag_map,
            &self.global_config.excenter_config.instance_id,
        );
    }

    async fn record_balance_total(&mut self) -> Result<()> {
        let now = now_ms();
        let last_record_time = self.last_record_time;

        //15s上报一次
        if now - last_record_time < 15000 {
            return Ok(());
        }
        self.last_record_time = now;
        let mut balance = 0.0;

        for (_, ctx) in self.contexts.iter() {
            balance += ctx.current_pos_usd_wealth;
        }

        let mut balance_ids = Vec::new();
        //ETH
        balance_ids.push(BalanceID {
            exchange: Exchange::ETHEREUM,
            account_type: AccountType::SPOT,
            currency: Currency::ETH,
        });

        let res = EX_MARKET
            .get_client(Exchange::ETHEREUM)
            .unwrap()
            .get_account(balance_ids)
            .await?;
        let mut native_coin_balance: f64 = 0.0;
        for (_, r) in res {
            native_coin_balance += r.balance;
        }

        let base_coin_price = get_currency_price(Currency::ETH)?;
        balance += base_coin_price * native_coin_balance;

        let mut field_map: HashMap<String, Value> = HashMap::new();
        let mut tag_map: HashMap<String, String> = HashMap::new();
        tag_map.insert("exchange".to_string(), "ETHEREUM".to_string());
        field_map.insert("balance_total_usdt".to_string(), json!(balance * 1.0));
        field_map.insert("balance_total_base".to_string(), json!(balance));
        EX_DATA_REPORTER.record_custom_data(
            "global_summary",
            field_map,
            tag_map,
            &self.global_config.excenter_config.instance_id,
        );
        let mut all_balance_usdt = balance;

        for (exchange, _) in self.assets_by_exchange.iter() {
            if exchange.is_defi() {
                continue;
            }
            let ak = self.ak_map.get(exchange).unwrap();
            let summary = self.excenter.get_global_summary(
                Currency::USDT,
                Some(ak.as_str()),
                GIT_TS.clone(),
                true,
            );
            let summary = summary.unwrap();
            log::info!("summary  :  {:?}", summary);
            let mut field_map: HashMap<String, Value> = HashMap::new();
            let mut tag_map: HashMap<String, String> = HashMap::new();
            tag_map.insert("exchange".to_string(), exchange.to_string());
            field_map.insert(
                "balance_total_usdt".to_string(),
                json!(summary.balance_total_usdt),
            );
            field_map.insert(
                "balance_total_base".to_string(),
                json!(summary.balance_total_base),
            );
            field_map.insert(
                "position_pure_usdt".to_string(),
                json!(summary.position_pure_usdt),
            );
            field_map.insert(
                "position_total_usdt".to_string(),
                json!(summary.position_total_usdt),
            );
            field_map.insert(
                "usage_lever_rate".to_string(),
                json!(summary.usage_lever_rate),
            );
            field_map.insert(
                "direction_lever_rate".to_string(),
                json!(summary.direction_lever_rate),
            );
            field_map.insert(
                "balance_total_base".to_string(),
                json!(summary.balance_total_base),
            );
            field_map.insert("mmr".to_string(), json!(summary.mmr));

            EX_DATA_REPORTER.record_custom_data(
                "global_summary",
                field_map,
                tag_map,
                &self.global_config.excenter_config.instance_id,
            );
            all_balance_usdt += summary.balance_total_usdt;
        }

        let mut field_map: HashMap<String, Value> = HashMap::new();
        let mut tag_map: HashMap<String, String> = HashMap::new();
        tag_map.insert("exchange".to_string(), "ALL".to_string());
        field_map.insert("balance_total_usdt".to_string(), json!(all_balance_usdt));
        field_map.insert("balance_total_base".to_string(), json!(all_balance_usdt));

        EX_DATA_REPORTER.record_custom_data(
            "global_summary",
            field_map,
            tag_map,
            &self.global_config.excenter_config.instance_id,
        );

        log::info!(
            "record_balance_total defi:{}  all:{}",
            balance,
            all_balance_usdt
        );
        Ok(())
    }
}

use tokio::time::interval;
impl LpStrategy {
    /// 运行策略
    pub async fn run(&mut self, _signal: mpsc::Receiver<EverestStrategySignal>) -> Result<()> {
        sleep(Duration::from_secs(10)).await;
        let mut market_event_stream = merge_market_event(&mut self.market_event_map);
        let mut user_event_stream = merge_user_event_list(&mut self.user_event_list);
        let mut sub_task_interval = interval(Duration::from_secs(3));
        let mut sub_tick_interval = interval(Duration::from_secs(15));
        'main: loop {
            tokio::select! {
                // _ = sleep(Duration::from_millis(500)) => {

                // },
                e = market_event_stream.next() => {
                    if e.is_none() {
                        log::error!("{}: market stream stopped", &self.instance_id);
                        break 'main;
                    }
                    let e = e.unwrap();
                    if e.is_err() {
                        continue;
                    }
                    let (asset, event) = e.unwrap();
                    if event == ExMarketEventType::ERROR {
                        log::error!("{}: market stream exit on asset {}, exit", &self.instance_id, asset.to_string());
                        break 'main;
                    }
                    let currency = asset.pair.0;
                    let (origin_currency, _) = currency.into_origin_currency();
                    let assets = self.assets_by_currency.get(&origin_currency);
                    if assets.is_none() {
                        continue;
                    } else {
                        let assets = assets.unwrap();
                        let cefi_assets: Vec<Asset> = assets.clone().into_iter().filter(|a| !a.exchange.is_defi() && a.asset_type == AssetType::SWAP).collect();
                        let defi_assets: Vec<Asset>  = assets.clone().into_iter().filter(|a| a.exchange.is_defi()).collect();
                        for cefi_asset in cefi_assets.iter() {
                            for defi_asset in defi_assets.iter() {
                                self.on_tick(cefi_asset.clone(), defi_asset.clone()).await?;
                            }
                        }
                    }
                },
                e = user_event_stream.next() => {
                    if e.is_none() {
                        log::error!("{}: user stream stopped", &self.instance_id);
                        break 'main;
                    }
                    let e = e.unwrap();
                    if e.is_err() {
                        log::warn!("{}: user context lagged! {:?}", &self.instance_id, e);
                        continue;
                    }

                    let (asset, event) = e.unwrap();
                    if event == ExUserDataEvent::ERROR {
                        log::error!("{}: user stream exit on asset {}, exit", &self.instance_id, asset.to_string());
                        break 'main;
                    }

                    let currency = asset.pair.0;
                    let (origin_currency, _) = currency.into_origin_currency();
                    let assets = self.assets_by_currency.get(&origin_currency);
                    if assets.is_none() {
                        continue;
                    } else {
                        let assets = assets.unwrap();
                        let cefi_assets: Vec<Asset> = assets.clone().into_iter().filter(|a| !a.exchange.is_defi() && a.asset_type == AssetType::SWAP).collect();
                        let defi_assets: Vec<Asset>  = assets.clone().into_iter().filter(|a| a.exchange.is_defi()).collect();
                        for cefi_asset in cefi_assets.iter() {
                            for defi_asset in defi_assets.iter() {
                                self.on_tick(cefi_asset.clone(), defi_asset.clone()).await?;
                            }
                        }
                    }
                },
                // 定期上报defi总资产
                 _ = sub_task_interval.tick() => {
                  self.record_balance_total().await?;
                },

                // 定期tick,防止币种无交易
                _ = sub_tick_interval.tick() => {
                    for (asset,_) in self.contexts.clone(){
                        let currency = asset.pair.0;
                        let (origin_currency, _) = currency.into_origin_currency();
                        let assets = self.assets_by_currency.get(&origin_currency);
                        if assets.is_none() {
                            continue;
                        } else {
                            let assets = assets.unwrap();
                            let cefi_assets: Vec<Asset> = assets.clone().into_iter().filter(|a| !a.exchange.is_defi() && a.asset_type == AssetType::SWAP).collect();
                            let defi_assets: Vec<Asset>  = assets.clone().into_iter().filter(|a| a.exchange.is_defi()).collect();
                            for cefi_asset in cefi_assets.iter() {
                                for defi_asset in defi_assets.iter() {
                                    self.on_tick(cefi_asset.clone(), defi_asset.clone()).await?;
                                }
                            }
                        }
                    }
                },
            }
        }
        Ok(())
    }
}

/// 初始化策略
pub async fn init_strategy(config: EverestStrategyConfig) -> Result<LpStrategy> {
    // 初始化交易中间层
    let mut excenter = ExCenter::new(config.excenter_config.clone());

    // 发送启动消息
    let excenter_config = &config.excenter_config;

    excenter.send_message_to_robot(
        &format!("策略启动 {}", excenter_config.instance_id),
        "----------------------",
        excenter_config.robot_host.as_deref(),
    );

    let _auto_filled_processor = OrderFilledFromMarketDataProcesser::default(); // 订单状态处理器
    let order_merge_processor = OrderMergeProcessor::default(); // 订单处理器
    let pos_sync_processor = PosSyncProcessor::default(); // 仓位同步处理器

    excenter.add_processor(Box::new(order_merge_processor));
    excenter.add_processor(Box::new(pos_sync_processor));

    for credential in config.credentials.iter() {
        let exchange = Exchange::from_str(&credential.exchange)?;
        excenter
            .login_exchange_with_credential(
                exchange,
                credential.get_excredential(),
                credential.alias.clone(),
            )
            .await?;
    }

    let strategy = LpStrategy::new(config, excenter).await.unwrap();
    Ok(strategy)
}

mod cefi;
pub mod config;
mod defi;
pub mod signal;
mod utils;
