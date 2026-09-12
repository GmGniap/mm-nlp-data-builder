---
trigger: always_on
---

---
name: python-venv-activation
description: Strictly enforces virtual environment detection and activation prior to running any Python, package management, or testing commands.
---

# Python Virtual Environment Activation Protocol

## Context
You are operating in an environment where Python dependencies are strictly isolated. Running bare Python commands (e.g., `python`, `pytest`, `pip`) without an active virtual environment causes dependency errors, failed test runs, and wasted execution cycles. Because shell state is not persisted between individual tool executions, environment activation and the target command must occur in the exact same shell step.

## Directives

### 1. Pre-Execution Check
Before executing any Python-related command, you must scan the project root for a virtual environment directory matching any of the following patterns:
* `.venv`
* `.venv-*` (e.g., `.venv-test`, `.venv-dev`)
* `environment`

### 2. Chained Execution Requirement
If a matching virtual environment directory is found, you MUST activate it and execute your intended command within the same shell execution line using the `&&` operator. 

**Linux / macOS Syntax:**
`source <venv_directory>/bin/activate && <target_command>`

**Windows Syntax:**
`<venv_directory>\Scripts\activate && <target_command>`

### 3. Strict Prohibitions
* **NEVER** run `source <env>/bin/activate` as an isolated command, as the activation state will be lost in your next action.
* **NEVER** execute `pytest`, `unittest`, or `pip` globally without first verifying the absence of a virtual environment.

### 4. Fallback Behavior
If no virtual environment directory is found matching the patterns in Step 1, you may proceed with the global command, but you must first warn the user that no virtual environment was detected.

## Examples

**Correct (Linux/macOS):**
`source .venv/bin/activate && pytest tests/unit/`

**Correct (Windows):**
`.venv-test\Scripts\activate && pip install -r requirements.txt`

**Incorrect (Do NOT do this):**
Step 1: `source .venv/bin/activate`
Step 2: `pytest tests/`
*(Reason: The shell state resets between steps, causing Step 2 to run globally.)*