#!/usr/bin/env python3
"""
super_chatgpt.py - A "super" Python ChatGPT client demo.

Features:
- CLI chat loop with conversation history
- Save/load conversations to JSON
- Streaming response support if the OpenAI client supports it
- Sandbox execution for Python code snippets with resource limits (Unix)
- Configurable model
- Helpful prompt/system-message scaffolding
"""

import os
import sys
import json
import time
import tempfile
import threading
from pathlib import Path
from typing import List, Dict, Any, Optional
import subprocess
import shlex

# ======= If you use the official OpenAI python package, uncomment this =======
# pip install openai
try:
    import openai
except Exception:
    openai = None

# -------------------------
# Configuration / Defaults
# -------------------------
DEFAULT_MODEL = os.environ.get("SUPER_CHAT_MODEL", "gpt-5-thinking-mini")  # replace if needed
CONVERSATIONS_DIR = Path.home() / ".super_chatgpt"
CONVERSATIONS_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_CONV_FILE = CONVERSATIONS_DIR / "default_conversation.json"
API_KEY = os.environ.get("OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY_OPENAI")  # support multiple names

if not API_KEY:
    print("Warning: OPENAI_API_KEY not set. You must set it to use the API.")
else:
    if openai:
        openai.api_key = API_KEY


# -------------------------
# Helpers
# -------------------------
def save_conversation(messages: List[Dict[str, str]], filename: Path = DEFAULT_CONV_FILE):
    """Save conversation (list of messages) to JSON."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(messages, f, indent=2, ensure_ascii=False)
    print(f"[saved] conversation to {filename}")


def load_conversation(filename: Path = DEFAULT_CONV_FILE) -> List[Dict[str, str]]:
    """Load conversation messages from JSON file; returns a list of messages."""
    if filename.exists():
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
    return []


def pretty_print_msg(role: str, text: str):
    prefix = {"system": "[SYSTEM]", "user": "[YOU]", "assistant": "[ASSISTANT]"}
    p = prefix.get(role, f"[{role.upper()}]")
    print(f"\n{p} {text}\n")


# -------------------------
# Minimal safe python execution
# -------------------------
def run_python_sandbox(code: str, timeout: int = 4) -> Dict[str, Any]:
    """
    Execute Python code in a subprocess with a timeout.
    WARNING: sandbox is lightweight. Do NOT run untrusted code from strangers in production.
    This will run "python -I" in a subprocess (isolated mode) and capture stdout/stderr.
    On Unix systems we also attempt to apply resource limits if available.
    """
    # Create a temporary file containing the code
    with tempfile.NamedTemporaryFile("w", suffix=".py", delete=False) as tf:
        tf.write(code)
        tmp_path = tf.name

    python_cmd = [sys.executable, "-I", tmp_path]  # -I isolates environment vars a bit
    proc = None
    try:
        # On Unix we can set resource limits via preexec_fn
        preexec_fn = None
        try:
            import resource

            def _limit():
                # limit CPU time (seconds)
                resource.setrlimit(resource.RLIMIT_CPU, (2, 2))
                # limit memory (address space) to e.g., 200MB
                resource.setrlimit(resource.RLIMIT_AS, (200 * 1024 * 1024, 200 * 1024 * 1024))
            preexec_fn = _limit
        except Exception:
            preexec_fn = None  # not available on Windows

        proc = subprocess.Popen(
            python_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            preexec_fn=preexec_fn,
        )
        stdout, stderr = proc.communicate(timeout=timeout)
        return {"returncode": proc.returncode, "stdout": stdout, "stderr": stderr}
    except subprocess.TimeoutExpired:
        if proc:
            proc.kill()
        return {"error": "timeout", "stdout": "", "stderr": "Execution timed out"}
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


# -------------------------
# Chat client (OpenAI)
# -------------------------
class SuperChat:
    def __init__(self, model: str = DEFAULT_MODEL, system_prompt: Optional[str] = None):
        self.model = model
        self.messages: List[Dict[str, str]] = []
        if system_prompt is None:
            system_prompt = (
                "You are a helpful, concise, and safe AI assistant. When the user asks to run python code, "
                "wrap the code with triple backticks and mark it with the language. Provide explanations when needed. "
                "If asked for potentially dangerous content, refuse and provide alternatives."
            )
        self.system_prompt = system_prompt
        self._ensure_system_message()

    def _ensure_system_message(self):
        if not any(m["role"] == "system" for m in self.messages):
            self.messages.insert(0, {"role": "system", "content": self.system_prompt})

    def add_user_message(self, text: str):
        self.messages.append({"role": "user", "content": text})

    def add_assistant_message(self, text: str):
        self.messages.append({"role": "assistant", "content": text})

    def save(self, filename: Path = DEFAULT_CONV_FILE):
        save_conversation(self.messages, filename)

    def load(self, filename: Path = DEFAULT_CONV_FILE):
        self.messages = load_conversation(filename)
        self._ensure_system_message()

    def ask(self, prompt: str, stream: bool = False) -> str:
        """
        Send the conversation + prompt to the API, receive text.
        If streaming is requested and available, stream content to stdout progressively.
        Returns full assistant text.
        """
        self.add_user_message(prompt)

        if openai is None:
            # Fallback local toy response (no API)
            assistant_text = "[toy assistant response] (no openai package installed)"
            self.add_assistant_message(assistant_text)
            return assistant_text

        # Standard ChatCompletion call
        try:
            if stream and hasattr(openai.ChatCompletion, "create"):
                # try streaming using the typical pattern
                # (note: actual streaming APIs and interfaces vary by SDK version)
                response_text = ""
                print("[streaming response] ", end="", flush=True)
                stream_resp = openai.ChatCompletion.create(
                    model=self.model,
                    messages=self.messages,
                    stream=True,
                    temperature=0.2,
                    max_tokens=800,
                )
                # stream_resp is an iterator of chunks
                for chunk in stream_resp:
                    # chunk parsing depends on SDK; common pattern:
                    # chunk.choices[0].delta.get("content", "")
                    try:
                        delta = chunk.choices[0].delta
                        content_piece = delta.get("content", "")
                    except Exception:
                        # fallback if chunk is a dict-like
                        content_piece = ""
                        try:
                            content_piece = chunk["choices"][0]["delta"].get("content", "")
                        except Exception:
                            pass
                    if content_piece:
                        print(content_piece, end="", flush=True)
                        response_text += content_piece
                print("")  # newline after stream finishes
                self.add_assistant_message(response_text)
                return response_text
            else:
                # non-streaming
                resp = openai.ChatCompletion.create(
                    model=self.model,
                    messages=self.messages,
                    temperature=0.2,
                    max_tokens=1000,
                )
                assistant_text = resp.choices[0].message["content"]
                self.add_assistant_message(assistant_text)
                return assistant_text
        except Exception as e:
            err = f"[API error] {e}"
            self.add_assistant_message(err)
            return err


# -------------------------
# CLI
# -------------------------
def repl_loop():
    print("Super ChatGPT CLI — type your message, ':exit' to quit, ':save' to save, ':load' to load, ':run' to run python snippet.")
    sc = SuperChat()
    # load last conversation if exists
    if DEFAULT_CONV_FILE.exists():
        sc.load(DEFAULT_CONV_FILE)
        print(f"[loaded] previous conversation from {DEFAULT_CONV_FILE}. Type ':history' to view or continue.")

    while True:
        try:
            user_in = input("You> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[exiting]")
            break

        if not user_in:
            continue

        if user_in.lower() in (":exit", ":quit"):
            print("[bye]")
            break
        if user_in.lower() == ":save":
            sc.save()
            continue
        if user_in.lower() == ":load":
            sc.load()
            continue
        if user_in.lower() == ":history":
            for m in sc.messages:
                print(f"{m['role']}: {m['content'][:200].replace(chr(10),' ')}")
            continue
        if user_in.startswith(":run "):
            # run code after the :run prefix
            code = user_in[len(":run ") :]
            print("[running code snippet]")
            r = run_python_sandbox(code)
            print("=== STDOUT ===")
            print(r.get("stdout", ""))
            print("=== STDERR ===")
            print(r.get("stderr", ""))
            continue
        if user_in == ":runblock":
            print("Enter Python code. End with a single line containing 'EOF'")
            lines = []
            while True:
                try:
                    line = input()
                except (EOFError, KeyboardInterrupt):
                    break
                if line.strip() == "EOF":
                    break
                lines.append(line)
            code = "\n".join(lines)
            print("[running code block]")
            r = run_python_sandbox(code)
            print("=== STDOUT ===")
            print(r.get("stdout", ""))
            print("=== STDERR ===")
            print(r.get("stderr", ""))
            continue

        # Ask model
        # detect if user wants streaming
        stream = False
        if user_in.startswith("!stream "):
            stream = True
            user_in = user_in[len("!stream ") :]

        print("[thinking...]")
        assistant_text = sc.ask(user_in, stream=stream)
        pretty_print_msg("assistant", assistant_text)


# -------------------------
# Example usage as a library
# -------------------------
def example_programmatic_usage():
    print("Example programmatic usage of SuperChat (requires openai package and valid API key).")
    sc = SuperChat(model=DEFAULT_MODEL)
    sc.add_user_message("Write a short Python function that returns prime numbers under n.")
    helper = sc.ask("Please provide the function and a short explanation.")
    print(helper)


# -------------------------
# Entry point
# -------------------------
def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--example":
        example_programmatic_usage()
        return
    repl_loop()


if __name__ == "__main__":
    main()
