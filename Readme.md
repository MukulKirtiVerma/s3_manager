# 🗂️ Advanced S3 Bulk Uploader with Folder Picker (GUI)

A Python GUI tool to **upload multiple files** from a local directory to a specific **AWS S3 bucket and folder (prefix)** — with **recursive S3 folder listing**, user-entered credentials, and intuitive file selection.  
🔒 File deletion is disabled for safety.

---

## ✨ Features

✅ Upload multiple files to a selected S3 folder (prefix)  
✅ Browse and select a local folder (non-recursive)  
✅ Enable/disable file uploads per file  
✅ Input AWS credentials securely via UI  
✅ Select from **available S3 buckets and folders (prefixes)**  
✅ View recursive folder structure from S3  
🚫 **Deletion from S3 is restricted** (disabled button)

---

## 🖥️ UI Preview

| AWS Auth & Bucket Selection | File Selection & Upload |
|----------------------------|-------------------------|
| ![Auth UI](./screenshots/aws_ui.png) | ![File UI](./screenshots/file_ui.png) |

> _Add your own screenshots under the `/screenshots` folder._

---

## 🛠️ Installation

### Prerequisites
- Python 3.7+
- `boto3` and `tkinter` (comes preinstalled with most Python setups)

### Install dependencies

```bash
pip install boto3
