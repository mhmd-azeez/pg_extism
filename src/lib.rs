use extism::*;
use extism_manifest::*;
use pgx::{prelude::*, Json};
use std::collections::BTreeMap;
use serde::{Serialize, Deserialize};

pgx::pg_module_magic!();

#[derive(Debug, Serialize, Deserialize)]
enum Type {
    String,
    Number,
    Json,
    StringArray,
    NumberArray,
    JsonArray,
}

#[derive(Serialize, Deserialize)]
struct PluginMetadata {
    #[serde(rename = "entryPoint")]
    entry_point: String,
    parameters: BTreeMap<String, Type>,
    #[serde(rename = "returnType")]
    return_type: Type,
    #[serde(rename = "returnField")]
    return_field: String,
}

#[pg_extern]
fn extism_call(path: &str, name: &str, input: Json) -> Result<Json, Error> {
    let json_string = serde_json::to_string(&input.0).unwrap();

    let ctx = Context::new();
    let mut plugin = new_plugin(&ctx, path);

    let data = match plugin.call(name, json_string) {
        Ok(v) => v,
        Err(e) => error!("Error while calling plugin: {}", e),
    };

    let output = match std::str::from_utf8(data) {
        Ok(v) => v,
        Err(e) => error!("Invalid UTF-8 sequence: {}", e),
    };

    let response_json: serde_json::Value = serde_json::from_str(output).unwrap();

    Ok(pgx::Json(response_json))
}

#[pg_extern]
fn extism_define(path: &str, name: &str) -> Result<(), Error> {
    let ctx = Context::new();
    let mut plugin = new_plugin(&ctx, path);

    if !plugin.has_function("metadata") {
        return Err(error!("Expected a `metadata` function."));
    }

    let metadata_json = match plugin.call("metadata", "") {
        Ok(v) => v,
        Err(err) => return Err(error!("Failed to call metadata function: {}", err)),
    };

    let metadata : PluginMetadata = match serde_json::from_slice(metadata_json) {
        Ok(v) => v,
        Err(err) => return Err(error!("Failed to deserialize metadata: {}", err)),
    };

    let sql = generate_dynamic_function(path, name, &metadata);
    Ok(pgx::Spi::run(&sql)?)
}

fn generate_dynamic_function(path: &str, name: &str, metadata: &PluginMetadata) -> String {
    let mut sql = format!("CREATE OR REPLACE FUNCTION {}(", name);

    let mut params_sql = Vec::new();

    for (param_name, param_type) in &metadata.parameters {
        params_sql.push(format!("{} {}", param_name, type_to_sql(param_type)));
    }

    params_sql.reverse();

    sql.push_str(&params_sql.join(", "));
    sql.push_str(&format!(
        ") RETURNS {} AS $$\n",
        type_to_sql(&metadata.return_type)
    ));

    sql.push_str("DECLARE\n");
    sql.push_str("    result_json json;\n");
    sql.push_str("    input_param json;\n");
    sql.push_str("BEGIN\n");
    sql.push_str("    -- Construct JSON object from parameters\n");
    sql.push_str("    input_param := json_build_object(\n");

    let mut params = Vec::new();

    for (param_name, _) in &metadata.parameters {
        params.push(format!("\t'{}', {}", param_name, param_name));
    }

    sql.push_str(&params.join(",\n"));

    sql.push_str("\n\t);\n");
    sql.push_str("    -- Call the extism_define function using the provided parameters\n");
    sql.push_str(&format!(
        "    SELECT extism_call('{}', '{}', input_param) INTO result_json;\n",
        path, metadata.entry_point
    ));
    sql.push_str("    -- Return the desired field from the result JSON\n");
    sql.push_str(&format!(
        "    RETURN (result_json->>'{}')::{};\n",
        metadata.return_field,
        type_to_sql(&metadata.return_type)
    ));
    sql.push_str("EXCEPTION\n");
    sql.push_str("    WHEN others THEN\n");
    sql.push_str("        -- Handle exceptions if necessary\n");
    sql.push_str("        RAISE NOTICE 'An error occurred: %', SQLERRM;\n");
    sql.push_str("        RETURN NULL;\n");
    sql.push_str("END;\n");
    sql.push_str("$$ LANGUAGE plpgsql;");

    sql
}

fn type_to_sql(param_type: &Type) -> String {
    match param_type {
        Type::Number => "NUMERIC".to_owned(),
        Type::String => "TEXT".to_owned(),
        Type::StringArray => "TEXT[]".to_owned(),
        Type::NumberArray => "NUMERIC[]".to_owned(),
        Type::Json => "JSON".to_owned(),
        Type::JsonArray => "JSON[]".to_owned(),
    }
}

fn new_plugin<'a>(ctx: &'a Context, path: &'a str) -> Plugin<'a> {
    let manifest = Manifest::new(vec![Wasm::file(path)])
        .with_memory_options(MemoryOptions { max_pages: Some(5) })
        .with_allowed_host("api.openai.com")
        .with_allowed_path("/", "/")
        .with_config(
            vec![(
                "openai_apikey".to_string(),
                "".to_string(),
            )]
            .into_iter(),
        )
        .with_timeout(std::time::Duration::from_secs(10));

    return Plugin::new_with_manifest(ctx, &manifest, [], true).unwrap();
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgx::prelude::*;

    use crate::extism_call;

    #[pg_test]
    fn test_extism_call_count_vowels() {
        Spi::run("select extism_define('/mnt/d/dylibso/pg_extism/src/code.wasm', 'count_vowels');")
            .unwrap();
        let result = Spi::get_one::<i32>("select count_vowels('aaabbb')->'count';");
        assert_eq!(Ok(Some(3)), result);
    }
}

/// This module is required by `cargo pgx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
