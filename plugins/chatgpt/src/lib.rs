#![no_main]

use std::str::from_utf8;
use extism_pdk::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::BTreeMap;

#[derive(Debug, Serialize, Deserialize)]
enum Type {
    String,
    Number,
    StringArray,
    NumberArray,
    Json,
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

#[derive(Deserialize)]
struct ChatMessage {
  content: String,
}

#[derive(Deserialize)]
struct ChatChoice {
  message: ChatMessage,
}

#[derive(Deserialize)]
struct ChatResult {
  choices: Vec<ChatChoice>,
}

#[derive(Deserialize)]
struct Input {
  prompt: String,
  payload: String,
}

#[derive(Serialize)]
struct Output {
  response: String
}

#[plugin_fn]
pub unsafe fn metadata(_: ()) -> FnResult<Json<PluginMetadata>> {
    let mut parameters = BTreeMap::new();
    parameters.insert("prompt".to_string(), Type::String);
    parameters.insert("payload".to_string(), Type::String);

    let metadata = PluginMetadata {
        entry_point: "chatgpt".to_string(),
        parameters,
        return_type: Type::String,
        return_field: "response".to_string(),
    };

    Ok(Json(metadata))
}

#[plugin_fn]
pub unsafe fn chatgpt<'a>(input: Vec<u8>) -> FnResult<String> {

  let input: Input = serde_json::from_slice(&input).unwrap();

  let api_key = config::get("openai_apikey").expect("Could not find config key 'openai_apikey'");

  let req = HttpRequest::new("https://api.openai.com/v1/chat/completions")
      .with_header("Authorization", format!("Bearer {}", api_key))
      .with_header("Content-Type", "application/json")
      .with_method("POST");

  let req_body = json!({
    "model": "gpt-3.5-turbo",
    "messages": [
        {
          "role": "user",
          "content": input.prompt + " " + &input.payload,
        }
    ],
  });

  info!("LLM: Making Call to OpenAI {}", req_body);

  let res = http::request::<String>(&req, Some(req_body.to_string()))?;
  let body = res.body();
  let body = from_utf8(&body)?;
  let body: ChatResult = serde_json::from_str(body)?;

  info!("LLM: Received Response from  OpenAI {:#?}", body.choices[0].message.content);

  Ok(serde_json::to_string(&Output{
    response: body.choices[0].message.content.clone()
  }).unwrap())

}


