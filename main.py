import os
import boto3
import threading
from tkinter import *
from tkinter import filedialog, messagebox, ttk
from botocore.exceptions import ClientError


class S3UploaderApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Advanced S3 Bulk Uploader")
        self.root.geometry("750x700")

        # AWS fields
        self.aws_key = StringVar()
        self.aws_secret = StringVar()
        self.aws_region = StringVar(value="us-east-1")
        self.bucket_name = StringVar()
        self.selected_prefix = StringVar()

        self.s3_client = None
        self.file_vars = []
        self.file_paths = []
        self.prefixes = []
        self.buckets = []

        self.build_ui()

    def build_ui(self):
        # Credentials Frame
        creds_frame = LabelFrame(self.root, text="AWS Credentials", padx=10, pady=10)
        creds_frame.pack(padx=10, pady=5, fill='x')

        Entry(creds_frame, textvariable=self.aws_key, width=40).grid(row=0, column=1, padx=5)
        Label(creds_frame, text="Access Key").grid(row=0, column=0, sticky='e')
        Entry(creds_frame, textvariable=self.aws_secret, width=40, show="*").grid(row=1, column=1, padx=5)
        Label(creds_frame, text="Secret Key").grid(row=1, column=0, sticky='e')
        Entry(creds_frame, textvariable=self.aws_region, width=40).grid(row=2, column=1, padx=5)
        Label(creds_frame, text="Region").grid(row=2, column=0, sticky='e')

        Button(creds_frame, text="🔐 Connect to AWS", command=self.connect_aws).grid(row=3, columnspan=2, pady=5)

        # Bucket & Prefix Frame
        s3_frame = LabelFrame(self.root, text="S3 Bucket & Folder", padx=10, pady=10)
        s3_frame.pack(padx=10, pady=5, fill='x')

        Label(s3_frame, text="Bucket:").grid(row=0, column=0)
        self.bucket_dropdown = ttk.Combobox(s3_frame, textvariable=self.bucket_name, width=60)
        self.bucket_dropdown.grid(row=0, column=1, padx=5)
        self.bucket_dropdown.bind("<<ComboboxSelected>>", lambda e: self.fetch_prefixes())

        Label(s3_frame, text="Target Prefix (Folder):").grid(row=1, column=0)
        self.prefix_dropdown = ttk.Combobox(s3_frame, textvariable=self.selected_prefix, width=60)
        self.prefix_dropdown.grid(row=1, column=1, padx=5)

        Button(s3_frame, text="🔄 Refresh", command=self.fetch_prefixes).grid(row=2, columnspan=2)

        # File Selection
        Button(self.root, text="📂 Browse Local Folder", command=self.browse_folder, bg="#d9edf7").pack(pady=10)

        self.frame = Frame(self.root)
        self.frame.pack(fill=BOTH, expand=True, padx=10, pady=5)

        # Action Buttons
        action_frame = Frame(self.root)
        action_frame.pack(pady=10)
        Button(action_frame, text="⬆ Upload Selected Files", command=self.upload_files, bg="#dff0d8").pack(side=LEFT, padx=10)
        Button(action_frame, text="🛑 Delete Disabled", state=DISABLED, bg="#f5c6cb").pack(side=LEFT, padx=10)

    def connect_aws(self):
        try:
            self.s3_client = boto3.client(
                's3',
                region_name=self.aws_region.get(),
                aws_access_key_id=self.aws_key.get(),
                aws_secret_access_key=self.aws_secret.get()
            )
            buckets = self.s3_client.list_buckets()
            self.buckets = [b['Name'] for b in buckets.get('Buckets', [])]
            self.bucket_dropdown['values'] = self.buckets
            if self.buckets:
                self.bucket_name.set(self.buckets[0])
                self.fetch_prefixes()
            messagebox.showinfo("Connected", "Successfully connected to AWS.")
        except ClientError as e:
            messagebox.showerror("Connection Error", str(e))

    def fetch_prefixes(self):
        bucket = self.bucket_name.get()
        if not bucket:
            return

        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            prefixes = set()
            for page in paginator.paginate(Bucket=bucket):
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if "/" in key:
                        prefix = "/".join(key.split("/")[:-1]) + "/"
                        prefixes.add(prefix)
            self.prefixes = sorted(prefixes)
            self.prefix_dropdown['values'] = self.prefixes
            if self.prefixes:
                self.selected_prefix.set(self.prefixes[0])
        except Exception as e:
            messagebox.showerror("Error", f"Error loading prefixes: {e}")

    def browse_folder(self):
        folder_path = filedialog.askdirectory()
        if folder_path:
            self.file_paths = [os.path.join(folder_path, f) for f in os.listdir(folder_path)
                               if os.path.isfile(os.path.join(folder_path, f))]
            self.file_vars.clear()
            for widget in self.frame.winfo_children():
                widget.destroy()
            for path in self.file_paths:
                var = BooleanVar(value=True)
                chk = Checkbutton(self.frame, text=os.path.basename(path), variable=var, anchor='w')
                chk.pack(fill='x', padx=10, pady=2)
                self.file_vars.append((var, path))

    def upload_files(self):
        selected_files = [path for var, path in self.file_vars if var.get()]
        if not selected_files:
            messagebox.showwarning("No Files", "Select files to upload.")
            return

        prefix = self.selected_prefix.get()
        bucket = self.bucket_name.get()

        def _upload():
            try:
                for file_path in selected_files:
                    filename = os.path.basename(file_path)
                    s3_key = os.path.join(prefix, filename) if prefix else filename
                    self.s3_client.upload_file(file_path, bucket, s3_key)
                messagebox.showinfo("Upload Complete", f"Uploaded {len(selected_files)} file(s) to {bucket}/{prefix}")
            except Exception as e:
                messagebox.showerror("Upload Error", str(e))

        threading.Thread(target=_upload).start()


if __name__ == "__main__":
    root = Tk()
    app = S3UploaderApp(root)
    root.mainloop()
