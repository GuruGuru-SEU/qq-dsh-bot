"""Render model Markdown as QQ-compatible plain text, preserving URLs and code."""
import re
from markdown_it import MarkdownIt

PARSER = MarkdownIt("commonmark", {"html": False}).enable(["table", "strikethrough"])


def inline(tokens):
    chunks, links = [], []
    for token in tokens or []:
        if token.type in ("text", "code_inline"):
            chunks.append(token.content)
        elif token.type in ("softbreak", "hardbreak"):
            chunks.append("\n")
        elif token.type == "link_open":
            links.append((token.attrGet("href"), len(chunks)))
        elif token.type == "link_close" and links:
            url, start = links.pop()
            if url and url not in "".join(chunks[start:]):
                chunks.append(" (" + url + ")")
        elif token.type == "image":
            chunks.append(token.content)
            if token.attrGet("src"):
                chunks.append(" (" + token.attrGet("src") + ")")
    return "".join(chunks)


def plain_reply(text):
    chunks = []
    for token in PARSER.parse(text):
        if token.type == "inline":
            chunks.append(inline(token.children))
        elif token.type in ("fence", "code_block"):
            chunks.append(token.content.rstrip() + "\n")
        elif token.type in ("paragraph_close", "heading_close", "list_item_close", "tr_close", "blockquote_close"):
            chunks.append("\n")
        elif token.type in ("th_close", "td_close"):
            chunks.append("  ")
    result = re.sub(r"\n{3,}", "\n\n", "".join(chunks)).strip()
    # A final backstop for runaway output; ordinary answers are prompted to be much shorter.
    if len(result) > 1800:
        result = result[:1760].rstrip() + "\n内容较长，先说到这里；需要的话可以继续问。"
    return result or "这次没有生成有效回答，请换个说法再试。"
