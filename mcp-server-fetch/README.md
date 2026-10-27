# mcp-server-fetch

Pinned build of the reference MCP `fetch` server for kagent (kmcp `MCPServer`, stdio). Used by the
`homelab-gitops` agent to read repository files as plain text from raw.githubusercontent.com, because the
GitHub MCP server returns file bodies only as embedded resources, which kagent's runtime does not forward
to the model.
