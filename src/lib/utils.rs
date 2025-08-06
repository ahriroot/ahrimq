use std::{
    env,
    error::Error,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// # Get current Unix timestamp
pub fn get_unix_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

/// Convert the config file path to an absolute path.
pub fn resolve_config_path(path: &str) -> Result<PathBuf, Box<dyn Error>> {
    let path = Path::new(path);

    // 如果是绝对路径，直接返回
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }

    // 获取当前工作目录
    let current_dir = env::current_dir()?;

    // 拼接成绝对路径
    let abs_path = current_dir.join(path);

    // 规范化路径（处理 ../ 和 ./ 等）
    let abs_path = normalize_path(&abs_path);

    Ok(abs_path)
}

/// Convert the path normalization.
pub fn normalize_path(path: &Path) -> PathBuf {
    let mut components = path.components();
    let mut normalized = PathBuf::new();

    while let Some(component) = components.next() {
        match component {
            std::path::Component::ParentDir => {
                if !normalized.pop() {
                    // 如果无法继续回退（已经到根目录），保留这个 ..
                    normalized.push("..");
                }
            }
            std::path::Component::CurDir => {
                // 忽略 .
            }
            _ => {
                normalized.push(component.as_os_str());
            }
        }
    }

    normalized
}
