# AI summaries run on the user's ChatGPT subscription via OAuth, with the bot's own token store

The bot analyzes Facebook posts with the user's ChatGPT Plus subscription (OAuth against the Codex backend, through
langchain-openai's experimental `_ChatOpenAICodex`) instead of a paid OpenAI API key. This means no per-token bill and
the same models the user already has. The bot keeps its own token store (`~/.aptbot/chatgpt-auth.json`, or the
`aptbot-ai` Docker volume) instead of reusing Codex CLI's `~/.codex/auth.json`, because two clients rotating one
refresh token log each other out.

## Consequences

- The integration is unofficial and uses a private class, so langchain-openai is pinned. Only
  `aptbot/listing_ai/llm/client.py` knows the backend, so moving to an API key (`ChatOpenAI`) or LiteLLM's `chatgpt/`
  provider means changing that one file.
- Usage counts against the plan's shared 5-hour Codex window. When the window is exhausted (HTTP 429
  `usage_limit_reached`), the bot pauses the AI until the reported reset and sends posts without a summary.
- langchain's own device-code login is broken in 1.8.5, so `llm/auth.py` implements Codex CLI's device protocol itself.
