use anyhow::{Ok, Result};
use excenter::prelude::{ExCenter, EX_MARKET};
// use exlib::ethers::abi::Hash;
use super::cefi::get_currency_price;
use exlib::prelude::*;

/// 获取 Defi的深度。如果是非USDT交易对，会转换成TOKEN/USDT的深度。 比如PEPE-ETH -》 PEPE-USDT
pub fn get_defi_depth(asset: &Asset) -> Result<Option<DepthData>> {
    let mut fake_asset = asset.clone();
    fake_asset.pair.1 = Currency::USDT;

    let mut depth;
    // 读取coin-base_coin深度
    {
        let ctx_arc = EX_MARKET.get_context(*asset)?;
        let ctx = ctx_arc.read();
        if ctx.depth.is_none() {
            return Ok(None);
        }
        depth = ctx.depth.as_ref().unwrap().clone();
    }
    // let pair1_usdt_price = get_base_coin_usdt_price(&asset)?;
    let pair1_usdt_price = 1.0;
    let price = depth.bids[0].price * pair1_usdt_price;
    depth.bids[0].price = price;
    depth.asks[0].price = price;
    depth.asks[0].volume = depth.asks[0].volume * pair1_usdt_price;

    Ok(Some(depth))
}

/// 获取defi账户等私有信息（余额和仓位）
pub fn get_defi_pos_and_account(
    excenter: &ExCenter,
    defi_asset: &Asset,
    defi_ak: &str,
) -> Result<Option<(PositionData, AccountData)>> {
    let mut defi_account;
    let defi_pos;
    {
        // log::info!("get_defi_pos_and_account  {:?}  ", defi_asset);
        // if defi_asset.token0.is_some() {
        //     log::info!(
        //         "get_defi_pos_and_account1  {:?} ",
        //         defi_asset.token0.unwrap()
        //     );
        // }
        let ctx_arc = excenter.get_context(*defi_asset, defi_ak)?;
        let ctx = ctx_arc.read();
        // if !ctx.is_post_safe() || !ctx.is_open_order_safe() {
        //     self.record_custom_data(currency, field_map, tag_map, false);
        //     return Ok(());
        // }
        defi_account = ctx.current_account.as_ref().unwrap().data.clone();
        defi_pos = ctx.current_position.as_ref().unwrap().data[0].clone();
        // defi中使用current position 代替 virtual position，防止balance比order先更新，导致pos不准。 ctx.virtual_position.as_ref().unwrap().data.clone();
    }

    let pair1_usdt_price = get_currency_price(defi_asset.pair.1)?;
    defi_account.balance *= pair1_usdt_price;

    Ok(Some((defi_pos, defi_account)))
}

pub fn get_defi_order_pending_and_end(
    excenter: &ExCenter,
    defi_asset: &Asset,
    defi_ak: &str,
) -> Result<(
    Option<OrderData>,
    Option<Vec<excenter::prelude::ExData<OrderData>>>,
)> {
    let ctx_arc = excenter.get_context(*defi_asset, defi_ak)?;
    let ctx = ctx_arc.read();
    let end_orders = ctx.ended_orders.clone();
    let id = defi_asset.to_string();

    let pending_order = if let Some(v) = ctx.pending_orders.get(&id) {
        Some(v.data.clone())
    } else {
        None
    };

    Ok((pending_order, Some(end_orders)))
}
