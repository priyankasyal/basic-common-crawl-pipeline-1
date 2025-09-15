"""Utility functions for Common Crawl processing."""


def count_file_lines(filename: str) -> int:
    """Count the number of lines in a file efficiently."""
    lines = 0
    with open(filename, "rb") as f:
        bufgen = iter(lambda: f.raw.read(1024 * 1024), b"")
        lines = sum(buf.count(b"\n") for buf in bufgen)
    return lines
