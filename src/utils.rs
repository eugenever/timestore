pub fn get_cwd() -> Result<String, anyhow::Error> {
    let cwd = std::env::current_dir()?;
    Ok(cwd.to_string_lossy().to_string())
}
