use crate::strategy::config::*;

/// 解析配置文件。文件路径从第一个命令行参数中获取
pub fn parse_config() -> EverestStrategyConfig {
    let args: Vec<String> = std::env::args().collect();
    let config: String = std::fs::read_to_string(&args[1]).unwrap();

    serde_json::from_str(&config).unwrap()
}
