use crate::config::Config;

pub async fn init_store(store_path: &str, config: &Config) -> Result<(), anyhow::Error> {
    // main db directory
    match tokio::fs::try_exists(&store_path).await {
        Err(err) => {
            return Err(anyhow::anyhow!("Error check exists 'store_path': {}", err));
        }
        Ok(exist) => {
            if !exist {
                if let Err(err) = tokio::fs::create_dir_all(&store_path).await {
                    return Err(anyhow::anyhow!("Error init LanceDB directory: {}", err));
                }
            }
        }
    }

    //Metadata directory
    let metadata_path = format!("{store_path}/metadata");
    match tokio::fs::try_exists(&metadata_path).await {
        Err(err) => {
            return Err(
                anyhow::anyhow!("Error check exists metadata directory '{metadata_path}': {}", err)
            );
        }
        Ok(exist) => {
            if !exist {
                if let Err(err) = tokio::fs::create_dir(&metadata_path).await {
                    return Err(
                        anyhow::anyhow!("Error init metadata directory '{metadata_path}': {}", err)
                    );
                }
            }
        }
    }

    // Datasets directories
    for dataset in &config.lancedb.datasets {
        let ds_path = format!("{store_path}/{dataset}");
        match tokio::fs::try_exists(&ds_path).await {
            Err(err) => {
                return Err(anyhow::anyhow!("Error check exists dataset '{dataset}': {}", err));
            }
            Ok(exist) => {
                if !exist {
                    if let Err(err) = tokio::fs::create_dir(&ds_path).await {
                        return Err(
                            anyhow::anyhow!("Error init dataset directory '{ds_path}': {}", err)
                        );
                    }
                }
            }
        }
    }

    // standarts directories
    if let Some(standarts) = &config.lancedb.standarts {
        for standart in standarts {
            let st_path = format!("{store_path}/measurements/{standart}");
            match tokio::fs::try_exists(&st_path).await {
                Err(err) => {
                    return Err(
                        anyhow::anyhow!("Error check exists standart path '{standart}': {}", err)
                    );
                }
                Ok(exist) => {
                    if !exist {
                        if let Err(err) = tokio::fs::create_dir_all(&st_path).await {
                            return Err(
                                anyhow::anyhow!(
                                    "Error init standart directory '{st_path}': {}",
                                    err
                                )
                            );
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
