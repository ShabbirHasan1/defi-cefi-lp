use anyhow::{anyhow, Ok, Result};
use excenter::prelude::{ExCenter, EX_MARKET};
use exlib::prelude::*;
use exlib::utils::now_ms;

/// 获取原始币种和乘数。 比如1000PEPE-USDT，返回(PEPE-USDT, 1000)
pub fn get_origin_asset(asset: &Asset) -> (Asset, f64) {
    let mut origin_asset = asset.clone();
    let (origin_currency, multiplier) = asset.pair.0.into_origin_currency();
    origin_asset.pair.0 = origin_currency;
    return (origin_asset, multiplier);
}

/// 获取 asset 中pair 1的usdt价格。比如PEPE-ETH，pair1就是ETH，获取BINANCE ETH-USDT价格
pub fn get_base_coin_usdt_price(asset: &Asset) -> Result<f64> {
    let pair1_usdt_price =
        EX_MARKET.get_pair_price(asset.exchange, (asset.pair.1, Currency::USDT))?;
    return Ok(pair1_usdt_price);
}

/// 获取Cefi价格
pub fn get_currency_price(currency: Currency) -> Result<f64> {
    let price = EX_MARKET.get_pair_price(Exchange::BINANCE, (currency, Currency::USDT))?;
    return Ok(price);
}

/// 获取cefi资金费率
#[allow(dead_code)]
pub fn get_cefi_funding_rate(asset: &Asset, use_time_factor: bool) -> Result<f64> {
    let mut funding_rate = 0.0;
    let cefi_asset = asset.clone();
    let ctx = EX_MARKET.get_context(cefi_asset).unwrap();
    let ctx = ctx.read();

    if let Some(ref current_funding) = ctx.funding_rate {
        let now = now_ms();
        funding_rate = current_funding.funding_rate;
        #[cfg(feature = "bybitv5")]
        if cefi_asset.exchange == Exchange::BYBITV5 {
            if let Some(last) = ctx.history_funding_rate.last() {
                let funding_interval = current_funding.funding_interval.unwrap();
                let weight = if current_funding.funding_time < now {
                    // bybit funding time 有的品种会实时更新，有的不会。没有实时更新的品种，使用上次的资金费率
                    log::error!(
                        "get_cefi_funding_rate, asset: {:?}, last.funding_time: {}, last.funding_rate: {}, current.funding_time: {}, current.funding_rate: {}, current.funding_interval: {}, now:{}",
                        asset,
                        last.funding_time,
                        last.funding_rate,
                        current_funding.funding_time,
                        current_funding.funding_rate,
                        funding_interval,
                        now,
                    );
                    if current_funding.funding_time + funding_interval + 3 * 60 * 1000 < now {
                        // 超过3分钟依然没有更新资金费率，返回错误，重启策略
                        return Err(anyhow!("bybitv5 funding rate not update"));
                    }
                    0.0
                } else {
                    1.0 - ((current_funding.funding_time - now) as f64 / funding_interval as f64)
                };

                let weight = (2.0 * weight).min(1.0);
                funding_rate =
                    last.funding_rate * (1.0 - weight) + current_funding.funding_rate * weight;
                // log::info!(
                //     "get_cefi_funding_rate, asset: {:?}, funding_rate: {}, last: {}, current: {}, weight: {}",
                //     asset,
                //     funding_rate,
                //     last.funding_rate,
                //     current_funding.funding_rate,
                //     weight);
            }
        }

        // process time factor(0.0 ~ 1.0)
        if use_time_factor {
            let funding_interval = current_funding
                .funding_interval
                .unwrap_or(8 * 60 * 60 * 1000);
            let weight = if current_funding.funding_time < now {
                0.0
            } else {
                1.0 - ((current_funding.funding_time - now) as f64 / funding_interval as f64)
            };
            let weight = weight.max(0.0).min(1.0) - 0.5;
            funding_rate = funding_rate * weight;
        }
    }
    Ok(funding_rate)
}

/// 获取 cefi 正常币对/乘数币对的深度
pub fn get_cefi_depth(asset: &Asset) -> Result<Option<DepthData>> {
    // 构建orgin asset
    let (origin_asset, multiplier) = get_origin_asset(asset);

    // 构建depth
    let mut origin_asset_depth;
    // 读取token-usdt深度
    {
        let ctx_arc = EX_MARKET.get_context(*asset)?;
        let ctx = ctx_arc.read();
        if ctx.depth.is_none() {
            return Ok(None);
        }
        origin_asset_depth = ctx.depth.as_ref().unwrap().clone();
    }
    origin_asset_depth.asset = origin_asset;
    // 价格除以乘数，数量乘以乘数
    for record in origin_asset_depth.asks.iter_mut() {
        record.price /= multiplier;
        record.volume *= multiplier;
    }
    for record in origin_asset_depth.bids.iter_mut() {
        record.price /= multiplier;
        record.volume *= multiplier;
    }

    return Ok(Some(origin_asset_depth));
}

/// 获取账户余额和仓位
pub fn get_cefi_pos_and_account(
    excenter: &ExCenter,
    cefi_asset: &Asset,
    cefi_ak: &str,
) -> Result<Option<(PositionData, AccountData)>> {
    // 获取cefi
    let ctx_arc = excenter.get_context(*cefi_asset, cefi_ak)?;
    let ctx = ctx_arc.read();
    // 如果有未成交的订单，则跳过
    if !ctx.is_post_safe() || !ctx.is_open_order_safe() {
        return Ok(None);
    }

    let cefi_account = ctx.current_account.as_ref().unwrap().data.clone();

    let (currency, multiplier) = cefi_asset.pair.0.into_origin_currency();
    let mut cefi_pos = ctx.virtual_position.as_ref().unwrap().data.clone();
    cefi_pos.asset.pair.0 = currency;
    cefi_pos.volume *= multiplier;

    return Ok(Some((cefi_pos, cefi_account)));
}
