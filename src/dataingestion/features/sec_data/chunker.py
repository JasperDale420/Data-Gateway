import re
from bs4 import BeautifulSoup
from typing import List, Dict


class TextChunker:
    """
    Cleans and chunks SEC filing text.
    """

    def __init__(self, chunk_size: int = 1000, overlap: int = 200):
        self.chunk_size = chunk_size
        self.overlap = overlap

    def clean_html(self, html_content: str) -> str:
        """
        Remove HTML tags and clean whitespace.
        """
        soup = BeautifulSoup(html_content, "html.parser")
        text = soup.get_text(separator="\n")

        # Clean excessive whitespace
        text = re.sub(r"\n\s*\n", "\n\n", text)
        text = re.sub(r"[ \t]+", " ", text)

        return text.strip()

    def chunk_text(self, text: str) -> List[Dict[str, str]]:
        """
        Split text into chunks.
        Returns: List of {"text": str, "index": int}
        """
        chunks = []
        start = 0
        text_len = len(text)

        idx = 0
        while start < text_len:
            end = start + self.chunk_size
            chunk_text = text[start:end]

            # Try to break at a newline or space
            if end < text_len:
                last_newline = chunk_text.rfind("\n")
                if last_newline != -1 and last_newline > self.chunk_size * 0.5:
                    end = start + last_newline + 1
                else:
                    last_space = chunk_text.rfind(" ")
                    if last_space != -1 and last_space > self.chunk_size * 0.5:
                        end = start + last_space + 1

            final_chunk = text[start:end].strip()
            if final_chunk:
                chunks.append({"text": final_chunk, "index": idx})
                idx += 1

            start = end - self.overlap

        return chunks


text_chunker = TextChunker()
