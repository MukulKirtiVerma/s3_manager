
import boto3
import threading
import time
from datetime import datetime
from tkinter import *
from tkinter import filedialog, messagebox, ttk
from botocore.exceptions import ClientError
import json
import mimetypes
import os
from dotenv import load_dotenv
import os

class EnhancedS3FileManager:
    def __init__(self, root):
        self.root = root
        self.root.title("Enhanced S3 File Manager")
        self.root.geometry("1400x900")
        self.root.configure(bg='#f0f0f0')


        # Load the .env file
        load_dotenv()

        # Access the variables
        aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
        aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
        aws_region = os.getenv("AWS_REGION")

        # AWS Configuration
        self.aws_key = StringVar(value=aws_access_key)
        self.aws_secret = StringVar(value=aws_secret_key)
        self.aws_region = StringVar(value=aws_region)
        self.bucket_name = StringVar(value="fitsol-ml-dev")
        self.current_path = StringVar(value="/")  # Current S3 path

        self.s3_client = None
        self.s3_resource = None
        self.is_connected = False
        self.upload_progress = IntVar()

        # File management
        self.current_objects = []
        self.selected_items = []
        self.navigation_history = []
        self.history_index = -1

        # Load saved settings
        self.load_settings()

        self.build_ui()

    def load_settings(self):
        """Load saved AWS settings"""
        try:
            if os.path.exists("s3_settings.json"):
                with open("s3_settings.json", "r") as f:
                    settings = json.load(f)
                    self.aws_key.set(settings.get("access_key", ""))
                    self.aws_region.set(settings.get("region", "us-east-1"))
        except Exception as e:
            print(f"Error loading settings: {e}")

    def save_settings(self):
        """Save AWS settings (excluding secret key for security)"""
        try:
            settings = {
                "access_key": self.aws_key.get(),
                "region": self.aws_region.get()
            }
            with open("s3_settings.json", "w") as f:
                json.dump(settings, f, indent=2)
            messagebox.showinfo("Settings", "Settings saved successfully!")
        except Exception as e:
            messagebox.showerror("Error", f"Error saving settings: {e}")

    def build_ui(self):
        # Define consistent color scheme and fonts
        self.colors = {
            'bg_primary': '#f8f9fa',
            'bg_secondary': '#e9ecef',
            'bg_accent': '#dee2e6',
            'text_primary': '#212529',
            'text_secondary': '#495057',
            'text_muted': '#6c757d',
            'border': '#ced4da',
            'success': '#198754',
            'danger': '#dc3545',
            'warning': '#fd7e14',
            'info': '#0dcaf0'
        }

        self.fonts = {
            'default': ('Segoe UI', 9),
            'bold': ('Segoe UI', 9, 'bold'),
            'heading': ('Segoe UI', 10, 'bold'),
            'mono': ('Consolas', 9),
            'small': ('Segoe UI', 8)
        }

        # Configure root window
        self.root.configure(bg=self.colors['bg_primary'])

        # Create main container with custom styling
        style = ttk.Style()
        style.theme_use('clam')

        # Configure ttk styles
        style.configure('Title.TLabel',
                        font=self.fonts['heading'],
                        foreground=self.colors['text_primary'])

        style.configure('Heading.TLabel',
                        font=self.fonts['bold'],
                        foreground=self.colors['text_primary'])

        # Top Frame - AWS Credentials
        self.create_credentials_frame()

        # Middle Frame - Path Navigation
        self.create_navigation_frame()

        # Main Frame - File Browser
        self.create_file_browser_frame()

        # Bottom Frame - Status and Actions
        self.create_status_frame()

    def create_credentials_frame(self):
        """Create AWS credentials input frame"""
        creds_frame = LabelFrame(self.root, text="🔐 AWS Credentials",
                                 padx=15, pady=10,
                                 bg=self.colors['bg_secondary'],
                                 fg=self.colors['text_primary'],
                                 font=self.fonts['heading'])
        creds_frame.pack(padx=10, pady=5, fill='x')

        # Grid layout for credentials
        creds_grid = Frame(creds_frame, bg=self.colors['bg_secondary'])
        creds_grid.pack(fill='x')

        # Labels with consistent styling
        Label(creds_grid, text="Access Key:",
              bg=self.colors['bg_secondary'],
              fg=self.colors['text_primary'],
              font=self.fonts['bold']).grid(row=0, column=0, sticky='e', padx=5)

        Entry(creds_grid, textvariable=self.aws_key, width=35, show="*",
              font=self.fonts['mono'],
              bg='white', fg=self.colors['text_primary'],
              insertbackground=self.colors['text_primary']).grid(row=0, column=1, padx=5, pady=2)

        Label(creds_grid, text="Secret Key:",
              bg=self.colors['bg_secondary'],
              fg=self.colors['text_primary'],
              font=self.fonts['bold']).grid(row=1, column=0, sticky='e', padx=5)

        Entry(creds_grid, textvariable=self.aws_secret, width=35, show="*",
              font=self.fonts['mono'],
              bg='white', fg=self.colors['text_primary'],
              insertbackground=self.colors['text_primary']).grid(row=1, column=1, padx=5, pady=2)

        Label(creds_grid, text="Region:",
              bg=self.colors['bg_secondary'],
              fg=self.colors['text_primary'],
              font=self.fonts['bold']).grid(row=2, column=0, sticky='e', padx=5)

        region_combo = ttk.Combobox(creds_grid, textvariable=self.aws_region, width=32,
                                    font=self.fonts['default'],
                                    values=['us-east-1', 'us-west-2', 'eu-west-1', 'ap-south-1', 'ap-southeast-1'])
        region_combo.grid(row=2, column=1, padx=5, pady=2)

        # Connection buttons with consistent styling
        btn_frame = Frame(creds_grid, bg=self.colors['bg_secondary'])
        btn_frame.grid(row=3, column=0, columnspan=3, pady=10)

        Button(btn_frame, text="🔗 Connect", command=self.connect_aws,
               bg=self.colors['success'], fg='white',
               font=self.fonts['bold'], padx=20,
               activebackground='#157347', activeforeground='white').pack(side=LEFT, padx=5)

        Button(btn_frame, text="💾 Save Settings", command=self.save_settings,
               bg=self.colors['info'], fg='white',
               font=self.fonts['default'],
               activebackground='#0aa2c0', activeforeground='white').pack(side=LEFT, padx=5)

        # Bucket selection with consistent styling
        Label(creds_grid, text="Bucket:",
              bg=self.colors['bg_secondary'],
              fg=self.colors['text_primary'],
              font=self.fonts['bold']).grid(row=0, column=2, sticky='e', padx=15)

        self.bucket_dropdown = ttk.Combobox(creds_grid, textvariable=self.bucket_name,
                                            width=25, state='readonly',
                                            font=self.fonts['default'])
        # self.bucket_dropdown.config(state='disabled')
        self.bucket_dropdown.grid(row=0, column=3, padx=5)
        self.bucket_dropdown.bind("<<ComboboxSelected>>", self.on_bucket_change)

    def create_navigation_frame(self):
        """Create Windows-like path navigation frame"""
        nav_frame = LabelFrame(self.root, text="📁 Path Navigation",
                               padx=10, pady=5,
                               bg=self.colors['bg_accent'],
                               fg=self.colors['text_primary'],
                               font=self.fonts['heading'])
        nav_frame.pack(padx=10, pady=5, fill='x')

        # Path breadcrumb
        path_frame = Frame(nav_frame, bg=self.colors['bg_accent'])
        path_frame.pack(fill='x', pady=5)

        # Navigation buttons with consistent styling
        self.back_btn = Button(path_frame, text="⬅", command=self.go_back,
                               state=DISABLED, width=3,
                               bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
                               font=self.fonts['default'],
                               activebackground=self.colors['bg_secondary'])
        self.back_btn.pack(side=LEFT, padx=2)

        Button(path_frame, text="⬆", command=self.go_up, width=3,
               bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
               font=self.fonts['default'],
               activebackground=self.colors['bg_secondary']).pack(side=LEFT, padx=2)

        Button(path_frame, text="🏠", command=self.go_home, width=3,
               bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
               font=self.fonts['default'],
               activebackground=self.colors['bg_secondary']).pack(side=LEFT, padx=2)

        Button(path_frame, text="🔄", command=self.refresh_current_path, width=3,
               bg=self.colors['bg_primary'], fg=self.colors['text_primary'],
               font=self.fonts['default'],
               activebackground=self.colors['bg_secondary']).pack(side=LEFT, padx=2)

        # Path display and input
        self.path_entry = Entry(path_frame, textvariable=self.current_path, width=60,
                                bg='white', fg=self.colors['text_primary'],
                                font=self.fonts['mono'],
                                insertbackground=self.colors['text_primary'])
        self.path_entry.pack(side=LEFT, padx=10, fill='x', expand=True)
        self.path_entry.bind('<Return>', lambda e: self.navigate_to_path())

        Button(path_frame, text="Go", command=self.navigate_to_path,
               bg=self.colors['warning'], fg='white',
               font=self.fonts['bold'],
               activebackground='#e85d04', activeforeground='white').pack(side=RIGHT, padx=2)

    def create_file_browser_frame(self):
        """Create main file browser with dual panes"""
        browser_frame = Frame(self.root)
        browser_frame.pack(padx=10, pady=5, fill='both', expand=True)

        # Create paned window for dual pane
        paned_window = ttk.PanedWindow(browser_frame, orient=HORIZONTAL)
        paned_window.pack(fill='both', expand=True)

        # Left pane - S3 Browser
        self.create_s3_browser_pane(paned_window)

        # Right pane - Local Files
        self.create_local_browser_pane(paned_window)

    def create_s3_browser_pane(self, parent):
        """Create S3 file browser pane"""
        s3_frame = LabelFrame(parent, text="☁️ S3 Files",
                              padx=5, pady=5,
                              bg=self.colors['bg_primary'],
                              fg=self.colors['text_primary'],
                              font=self.fonts['heading'])
        parent.add(s3_frame, weight=1)

        # Configure grid weights
        s3_frame.grid_rowconfigure(1, weight=1)
        s3_frame.grid_columnconfigure(0, weight=1)

        # Toolbar with consistent styling
        toolbar = Frame(s3_frame, bg=self.colors['bg_primary'])
        toolbar.grid(row=0, column=0, columnspan=2, sticky='ew', pady=5)

        # Toolbar buttons with consistent styling
        Button(toolbar, text="📁 New Folder", command=self.create_folder,
               bg=self.colors['info'], fg='white',
               font=self.fonts['default'],
               activebackground='#0aa2c0', activeforeground='white').pack(side=LEFT, padx=2)

        Button(toolbar, text="⬇ Download", command=self.download_selected,
               bg=self.colors['success'], fg='white',
               font=self.fonts['default'],
               activebackground='#157347', activeforeground='white').pack(side=LEFT, padx=2)

        Button(toolbar, text="🗑 Delete", command=self.delete_selected,
               bg=self.colors['danger'], fg='white',
               font=self.fonts['default'],
               activebackground='#b02a37', activeforeground='white').pack(side=LEFT, padx=2)

        Button(toolbar, text="📋 Copy Path", command=self.copy_path,
               bg=self.colors['warning'], fg='white',
               font=self.fonts['default'],
               activebackground='#e85d04', activeforeground='white').pack(side=LEFT, padx=2)

        # File list with columns
        columns = ('Name', 'Type', 'Size', 'Modified')
        self.s3_tree = ttk.Treeview(s3_frame, columns=columns, show='tree headings', height=15)

        # Configure columns
        self.s3_tree.heading('#0', text='📂')
        self.s3_tree.column('#0', width=30, minwidth=30)

        for col in columns:
            self.s3_tree.heading(col, text=col)
            if col == 'Name':
                self.s3_tree.column(col, width=300)
            elif col == 'Size':
                self.s3_tree.column(col, width=100)
            elif col == 'Modified':
                self.s3_tree.column(col, width=150)
            else:
                self.s3_tree.column(col, width=80)

        # Scrollbars
        v_scroll = ttk.Scrollbar(s3_frame, orient=VERTICAL, command=self.s3_tree.yview)
        h_scroll = ttk.Scrollbar(s3_frame, orient=HORIZONTAL, command=self.s3_tree.xview)
        self.s3_tree.configure(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)

        # Grid treeview and scrollbars
        self.s3_tree.grid(row=1, column=0, sticky='nsew')
        v_scroll.grid(row=1, column=1, sticky='ns')
        h_scroll.grid(row=2, column=0, sticky='ew')

        # Bind events
        self.s3_tree.bind('<Double-1>', self.on_s3_double_click)
        self.s3_tree.bind('<Button-3>', self.show_s3_context_menu)

    def create_local_browser_pane(self, parent):
        """Create local file browser pane"""
        local_frame = LabelFrame(parent, text="💻 Local Files",
                                 padx=5, pady=5,
                                 bg=self.colors['bg_primary'],
                                 fg=self.colors['text_primary'],
                                 font=self.fonts['heading'])
        parent.add(local_frame, weight=1)

        # Configure grid weights
        local_frame.grid_rowconfigure(2, weight=1)
        local_frame.grid_columnconfigure(0, weight=1)

        # Toolbar with consistent styling
        toolbar = Frame(local_frame, bg=self.colors['bg_primary'])
        toolbar.grid(row=0, column=0, columnspan=2, sticky='ew', pady=5)

        # Toolbar buttons with consistent styling
        Button(toolbar, text="📂 Browse", command=self.browse_local_folder,
               bg=self.colors['info'], fg='white',
               font=self.fonts['default'],
               activebackground='#0aa2c0', activeforeground='white').pack(side=LEFT, padx=2)

        Button(toolbar, text="⬆ Upload Selected", command=self.upload_selected,
               bg=self.colors['success'], fg='white',
               font=self.fonts['default'],
               activebackground='#157347', activeforeground='white').pack(side=LEFT, padx=2)

        Button(toolbar, text="⬆ Upload All", command=self.upload_all,
               bg='#20c997', fg='white',
               font=self.fonts['default'],
               activebackground='#1aa181', activeforeground='white').pack(side=LEFT, padx=2)

        # Local path display with consistent styling
        self.local_path_var = StringVar(value=os.getcwd())
        local_path_frame = Frame(local_frame, bg=self.colors['bg_primary'])
        local_path_frame.grid(row=1, column=0, columnspan=2, sticky='ew', pady=2)

        Label(local_path_frame, text="Local Path:",
              font=self.fonts['bold'],
              bg=self.colors['bg_primary'],
              fg=self.colors['text_primary']).pack(side=LEFT)

        Entry(local_path_frame, textvariable=self.local_path_var, state='readonly',
              bg=self.colors['bg_secondary'], fg=self.colors['text_secondary'],
              font=self.fonts['mono']).pack(side=LEFT, fill='x', expand=True, padx=5)

        # Local file list
        columns = ('Name', 'Type', 'Size', 'Modified')
        self.local_tree = ttk.Treeview(local_frame, columns=columns, show='tree headings', height=15)

        # Configure columns
        self.local_tree.heading('#0', text='📂')
        self.local_tree.column('#0', width=30, minwidth=30)

        for col in columns:
            self.local_tree.heading(col, text=col)
            if col == 'Name':
                self.local_tree.column(col, width=300)
            elif col == 'Size':
                self.local_tree.column(col, width=100)
            elif col == 'Modified':
                self.local_tree.column(col, width=150)
            else:
                self.local_tree.column(col, width=80)

        # Scrollbars for local tree
        local_v_scroll = ttk.Scrollbar(local_frame, orient=VERTICAL, command=self.local_tree.yview)
        local_h_scroll = ttk.Scrollbar(local_frame, orient=HORIZONTAL, command=self.local_tree.xview)
        self.local_tree.configure(yscrollcommand=local_v_scroll.set, xscrollcommand=local_h_scroll.set)

        # Grid local treeview and scrollbars
        self.local_tree.grid(row=2, column=0, sticky='nsew')
        local_v_scroll.grid(row=2, column=1, sticky='ns')
        local_h_scroll.grid(row=3, column=0, sticky='ew')

        # Bind events
        self.local_tree.bind('<Double-1>', self.on_local_double_click)

        # Load initial local directory
        self.refresh_local_files()

    def create_status_frame(self):
        """Create status and progress frame"""
        status_frame = Frame(self.root, bg=self.colors['bg_secondary'], relief=SUNKEN, bd=1)
        status_frame.pack(fill='x', padx=10, pady=5)

        # Status label with consistent styling
        self.status_label = Label(status_frame, text="Ready",
                                  bg=self.colors['bg_secondary'],
                                  fg=self.colors['text_primary'],
                                  anchor='w', font=self.fonts['default'])
        self.status_label.pack(side=LEFT, padx=10)

        # Progress bar
        self.progress_bar = ttk.Progressbar(status_frame, mode='determinate', length=300)
        self.progress_bar.pack(side=RIGHT, padx=10, pady=5)

        # Connection status with consistent styling
        self.connection_status = Label(status_frame, text="● Disconnected",
                                       fg=self.colors['danger'],
                                       bg=self.colors['bg_secondary'],
                                       font=self.fonts['bold'])
        self.connection_status.pack(side=RIGHT, padx=20)

    # AWS Connection Methods
    def connect_aws(self):
        """Connect to AWS S3"""
        try:
            self.update_status("Connecting to AWS...")

            # Validate inputs
            if not self.aws_key.get() or not self.aws_secret.get():
                messagebox.showerror("Error", "Please provide AWS credentials")
                return

            self.s3_client = boto3.client(
                's3',
                region_name=self.aws_region.get(),
                aws_access_key_id=self.aws_key.get(),
                aws_secret_access_key=self.aws_secret.get()
            )

            # Test connection
            response = self.s3_client.list_buckets()
            buckets = [b['Name'] for b in response.get('Buckets', [])]

            self.bucket_dropdown['values'] = "fitsol-ml-dev"
            if buckets:
                self.bucket_name.set(buckets[0])

            self.is_connected = True
            self.connection_status.config(text="● Connected", fg=self.colors['success'])
            self.update_status(f"Connected successfully. Found {len(buckets)} buckets.")

            # Load initial S3 content
            if buckets:
                self.refresh_s3_files()



            messagebox.showinfo("Success", f"Connected to AWS S3\nFound {len(buckets)} buckets")

        except ClientError as e:
            error_msg = str(e)
            self.connection_status.config(text="● Connection Failed", fg=self.colors['danger'])
            self.update_status("Connection failed")
            messagebox.showerror("AWS Error", f"Connection failed:\n{error_msg}")
        except Exception as e:
            self.connection_status.config(text="● Error", fg=self.colors['danger'])
            self.update_status("Unexpected error")
            messagebox.showerror("Error", f"Unexpected error:\n{str(e)}")

    def on_bucket_change(self, event=None):
        """Handle bucket selection change"""
        if self.is_connected:
            self.current_path.set("/")
            self.navigation_history = ["/"]
            self.history_index = 0
            self.refresh_s3_files()

    # Navigation Methods
    def add_to_history(self, path):
        """Add path to navigation history"""
        if self.history_index >= 0 and self.history_index < len(self.navigation_history):
            # Remove forward history when navigating to new path
            self.navigation_history = self.navigation_history[:self.history_index + 1]

        if not self.navigation_history or self.navigation_history[-1] != path:
            self.navigation_history.append(path)
            self.history_index = len(self.navigation_history) - 1

        self.update_navigation_buttons()

    def update_navigation_buttons(self):
        """Update navigation button states"""
        if self.history_index > 0:
            self.back_btn.config(state=NORMAL)
        else:
            self.back_btn.config(state=DISABLED)

    def go_back(self):
        """Go back in navigation history"""
        if self.history_index > 0:
            self.history_index -= 1
            path = self.navigation_history[self.history_index]
            self.current_path.set(path)
            self.refresh_s3_files()
            self.update_navigation_buttons()

    def go_up(self):
        """Go up one directory level"""
        current = self.current_path.get()
        if current != "/":
            parent = "/".join(current.rstrip("/").split("/")[:-1])
            if not parent:
                parent = "/"
            self.current_path.set(parent)
            self.add_to_history(parent)
            self.refresh_s3_files()

    def go_home(self):
        """Go to root directory"""
        self.current_path.set("/")
        self.add_to_history("/")
        self.refresh_s3_files()

    def navigate_to_path(self):
        """Navigate to the path in the entry"""
        path = self.current_path.get()
        if not path.startswith("/"):
            path = "/" + path
        self.current_path.set(path)
        self.add_to_history(path)
        self.refresh_s3_files()

    def refresh_current_path(self):
        """Refresh current path"""
        self.refresh_s3_files()

    # File Browser Methods
    def refresh_s3_files(self):
        """Refresh S3 file listing"""
        if not self.is_connected or not self.bucket_name.get():
            return

        try:
            self.update_status("Loading S3 files...")

            # Clear existing items
            for item in self.s3_tree.get_children():
                self.s3_tree.delete(item)

            bucket = self.bucket_name.get()
            prefix = self.current_path.get().lstrip("/")
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            # List objects
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=bucket,
                Prefix=prefix,
                Delimiter='/'
            )

            folders = set()
            files = []

            for page in page_iterator:
                # Add folders (common prefixes)
                for folder_info in page.get('CommonPrefixes', []):
                    folder_name = folder_info['Prefix'][len(prefix):].rstrip('/')
                    if folder_name:
                        folders.add(folder_name)

                # Add files
                for obj in page.get('Contents', []):
                    key = obj['Key']
                    if key != prefix:  # Don't show the current directory itself
                        file_name = key[len(prefix):]
                        if '/' not in file_name:  # Only direct children
                            files.append({
                                'name': file_name,
                                'key': key,
                                'size': obj['Size'],
                                'modified': obj['LastModified'].strftime('%Y-%m-%d %H:%M:%S'),
                                'type': 'File'
                            })

            # Insert folders first
            for folder in sorted(folders):
                self.s3_tree.insert('', 'end', text='📁', values=(folder, 'Folder', '', ''))

            # Insert files
            for file_info in sorted(files, key=lambda x: x['name']):
                size_str = self.format_file_size(file_info['size'])
                icon = self.get_file_icon(file_info['name'])
                self.s3_tree.insert('', 'end', text=icon, values=(
                    file_info['name'], file_info['type'], size_str, file_info['modified']
                ))

            self.update_status(f"Loaded {len(folders)} folders and {len(files)} files")

        except Exception as e:
            self.update_status("Error loading S3 files")
            messagebox.showerror("Error", f"Error loading S3 files:\n{str(e)}")

    def refresh_local_files(self):
        """Refresh local file listing"""
        try:
            # Clear existing items
            for item in self.local_tree.get_children():
                self.local_tree.delete(item)

            current_dir = self.local_path_var.get()

            # List directory contents
            items = os.listdir(current_dir)

            folders = []
            files = []

            for item in items:
                item_path = os.path.join(current_dir, item)
                try:
                    if os.path.isdir(item_path):
                        folders.append(item)
                    else:
                        stat = os.stat(item_path)
                        files.append({
                            'name': item,
                            'size': stat.st_size,
                            'modified': datetime.fromtimestamp(stat.st_mtime).strftime('%Y-%m-%d %H:%M:%S')
                        })
                except (OSError, PermissionError):
                    continue

            # Insert folders first
            for folder in sorted(folders):
                self.local_tree.insert('', 'end', text='📁', values=(folder, 'Folder', '', ''))

            # Insert files
            for file_info in sorted(files, key=lambda x: x['name']):
                size_str = self.format_file_size(file_info['size'])
                icon = self.get_file_icon(file_info['name'])
                self.local_tree.insert('', 'end', text=icon, values=(
                    file_info['name'], 'File', size_str, file_info['modified']
                ))

        except Exception as e:
            messagebox.showerror("Error", f"Error loading local files:\n{str(e)}")

    def on_s3_double_click(self, event):
        """Handle double-click on S3 tree item"""
        selection = self.s3_tree.selection()
        if selection:
            item = self.s3_tree.item(selection[0])
            name = item['values'][0]
            item_type = item['values'][1]

            if item_type == 'Folder':
                # Navigate to folder
                current = self.current_path.get().rstrip("/")
                new_path = f"{current}/{name}" if current != "/" else f"/{name}"
                self.current_path.set(new_path)
                self.add_to_history(new_path)
                self.refresh_s3_files()

    def on_local_double_click(self, event):
        """Handle double-click on local tree item"""
        selection = self.local_tree.selection()
        if selection:
            item = self.local_tree.item(selection[0])
            name = item['values'][0]
            item_type = item['values'][1]

            if item_type == 'Folder':
                # Navigate to folder
                new_path = os.path.join(self.local_path_var.get(), name)
                self.local_path_var.set(new_path)
                self.refresh_local_files()

    # File Operations
    def browse_local_folder(self):
        """Browse for local folder"""
        folder = filedialog.askdirectory(initialdir=self.local_path_var.get())
        if folder:
            self.local_path_var.set(folder)
            self.refresh_local_files()

    def create_folder(self):
        """Create new folder in S3"""
        if not self.is_connected:
            messagebox.showerror("Error", "Not connected to AWS")
            return

        folder_name = messagebox.askstring("New Folder", "Enter folder name:")
        if not folder_name:
            return

        try:
            bucket = self.bucket_name.get()
            current = self.current_path.get().lstrip("/")
            if current and not current.endswith("/"):
                current += "/"

            # Create empty object with trailing slash to represent folder
            s3_key = current + folder_name + "/"
            self.s3_client.put_object(Bucket=bucket, Key=s3_key, Body=b'')

            self.update_status(f"Created folder: {folder_name}")
            self.refresh_s3_files()
            messagebox.showinfo("Success", f"Folder '{folder_name}' created successfully")

        except Exception as e:
            messagebox.showerror("Error", f"Failed to create folder:\n{str(e)}")

    def upload_selected(self):
        """Upload selected local files"""
        selection = self.local_tree.selection()
        if not selection:
            messagebox.showwarning("No Selection", "Please select files to upload")
            return

        files_to_upload = []
        for item_id in selection:
            item = self.local_tree.item(item_id)
            if item['values'][1] == 'File':  # Only files, not folders
                files_to_upload.append(item['values'][0])

        if files_to_upload:
            self.upload_files(files_to_upload)

    def upload_all(self):
        """Upload all files in current local directory"""
        files_to_upload = []
        for item_id in self.local_tree.get_children():
            item = self.local_tree.item(item_id)
            if item['values'][1] == 'File':
                files_to_upload.append(item['values'][0])

        if files_to_upload:
            result = messagebox.askyesno("Confirm Upload", f"Upload {len(files_to_upload)} files to S3?")
            if result:
                self.upload_files(files_to_upload)

    def upload_files(self, file_names):
        """Upload files to S3"""
        if not self.is_connected:
            messagebox.showerror("Error", "Not connected to AWS")
            return

        def upload_worker():
            try:
                bucket = self.bucket_name.get()
                s3_prefix = self.current_path.get().lstrip("/")
                if s3_prefix and not s3_prefix.endswith("/"):
                    s3_prefix += "/"

                total_files = len(file_names)
                uploaded = 0

                for file_name in file_names:
                    local_path = os.path.join(self.local_path_var.get(), file_name)
                    s3_key = s3_prefix + file_name if s3_prefix else file_name

                    self.update_status(f"Uploading {file_name}...")

                    # Get file content type
                    content_type, _ = mimetypes.guess_type(local_path)
                    if not content_type:
                        content_type = 'binary/octet-stream'

                    # Upload with content type
                    self.s3_client.upload_file(
                        local_path, bucket, s3_key,
                        ExtraArgs={'ContentType': content_type}
                    )
                    uploaded += 1

                    # Update progress
                    progress = int((uploaded / total_files) * 100)
                    self.progress_bar['value'] = progress
                    self.root.update_idletasks()

                self.update_status(f"Successfully uploaded {uploaded} files")
                self.progress_bar['value'] = 0
                self.refresh_s3_files()
                messagebox.showinfo("Success", f"Uploaded {uploaded} files successfully")

            except Exception as e:
                self.update_status("Upload failed")
                self.progress_bar['value'] = 0
                messagebox.showerror("Upload Error", f"Upload failed:\n{str(e)}")

        threading.Thread(target=upload_worker, daemon=True).start()

    def download_selected(self):
        """Download selected S3 files"""
        selection = self.s3_tree.selection()
        if not selection:
            messagebox.showwarning("No Selection", "Please select files to download")
            return

        files_to_download = []
        for item_id in selection:
            item = self.s3_tree.item(item_id)
            if item['values'][1] == 'File':
                files_to_download.append(item['values'][0])

        if not files_to_download:
            messagebox.showwarning("No Files", "No files selected for download")
            return

        # Ask for download location
        download_dir = filedialog.askdirectory(title="Select Download Location")
        if not download_dir:
            return

        def download_worker():
            try:
                bucket = self.bucket_name.get()
                s3_prefix = self.current_path.get().lstrip("/")
                if s3_prefix and not s3_prefix.endswith("/"):
                    s3_prefix += "/"

                total_files = len(files_to_download)
                downloaded = 0

                for file_name in files_to_download:
                    s3_key = s3_prefix + file_name if s3_prefix else file_name
                    local_path = os.path.join(download_dir, file_name)

                    self.update_status(f"Downloading {file_name}...")

                    self.s3_client.download_file(bucket, s3_key, local_path)
                    downloaded += 1

                    # Update progress
                    progress = int((downloaded / total_files) * 100)
                    self.progress_bar['value'] = progress
                    self.root.update_idletasks()

                self.update_status(f"Successfully downloaded {downloaded} files")
                self.progress_bar['value'] = 0
                messagebox.showinfo("Success", f"Downloaded {downloaded} files to {download_dir}")

            except Exception as e:
                self.update_status("Download failed")
                self.progress_bar['value'] = 0
                messagebox.showerror("Download Error", f"Download failed:\n{str(e)}")

        threading.Thread(target=download_worker, daemon=True).start()

    def delete_selected(self):
        """Delete selected S3 files"""
        selection = self.s3_tree.selection()
        if not selection:
            messagebox.showwarning("No Selection", "Please select items to delete")
            return

        items_to_delete = []
        for item_id in selection:
            item = self.s3_tree.item(item_id)
            items_to_delete.append((item['values'][0], item['values'][1]))

        if not items_to_delete:
            return

        result = messagebox.askyesno("Confirm Delete",
                                     f"Are you sure you want to delete {len(items_to_delete)} items?\nThis action cannot be undone.")
        if not result:
            return

        def delete_worker():
            try:
                bucket = self.bucket_name.get()
                s3_prefix = self.current_path.get().lstrip("/")
                if s3_prefix and not s3_prefix.endswith("/"):
                    s3_prefix += "/"

                deleted = 0
                for item_name, item_type in items_to_delete:
                    self.update_status(f"Deleting {item_name}...")

                    if item_type == 'Folder':
                        # Delete folder and all its contents
                        folder_prefix = s3_prefix + item_name + "/"
                        self.delete_folder_recursive(bucket, folder_prefix)
                    else:
                        # Delete single file
                        s3_key = s3_prefix + item_name if s3_prefix else item_name
                        self.s3_client.delete_object(Bucket=bucket, Key=s3_key)

                    deleted += 1

                self.update_status(f"Successfully deleted {deleted} items")
                self.refresh_s3_files()
                messagebox.showinfo("Success", f"Deleted {deleted} items successfully")

            except Exception as e:
                self.update_status("Delete failed")
                messagebox.showerror("Delete Error", f"Delete failed:\n{str(e)}")

        threading.Thread(target=delete_worker, daemon=True).start()

    def delete_folder_recursive(self, bucket, prefix):
        """Recursively delete all objects in a folder"""
        try:
            # List all objects with the prefix
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

            objects_to_delete = []
            for page in page_iterator:
                for obj in page.get('Contents', []):
                    objects_to_delete.append({'Key': obj['Key']})

            # Delete objects in batches
            if objects_to_delete:
                # S3 allows up to 1000 objects per delete request
                for i in range(0, len(objects_to_delete), 1000):
                    batch = objects_to_delete[i:i + 1000]
                    self.s3_client.delete_objects(
                        Bucket=bucket,
                        Delete={'Objects': batch}
                    )
        except Exception as e:
            raise Exception(f"Failed to delete folder contents: {str(e)}")

    def copy_path(self):
        """Copy S3 path to clipboard"""
        selection = self.s3_tree.selection()
        if not selection:
            messagebox.showwarning("No Selection", "Please select an item to copy path")
            return

        item = self.s3_tree.item(selection[0])
        item_name = item['values'][0]

        bucket = self.bucket_name.get()
        current = self.current_path.get().lstrip("/")
        if current:
            s3_path = f"s3://{bucket}/{current}/{item_name}"
        else:
            s3_path = f"s3://{bucket}/{item_name}"

        # Copy to clipboard
        self.root.clipboard_clear()
        self.root.clipboard_append(s3_path)
        self.update_status(f"Copied path: {s3_path}")
        messagebox.showinfo("Path Copied", f"S3 path copied to clipboard:\n{s3_path}")

    def show_s3_context_menu(self, event):
        """Show context menu for S3 items"""
        selection = self.s3_tree.selection()
        if not selection:
            return

        context_menu = Menu(self.root, tearoff=0,
                            bg=self.colors['bg_primary'],
                            fg=self.colors['text_primary'],
                            font=self.fonts['default'],
                            activebackground=self.colors['bg_accent'],
                            activeforeground=self.colors['text_primary'])

        context_menu.add_command(label="Download", command=self.download_selected)
        context_menu.add_command(label="Delete", command=self.delete_selected)
        context_menu.add_separator()
        context_menu.add_command(label="Copy Path", command=self.copy_path)
        context_menu.add_command(label="Properties", command=self.show_properties)

        try:
            context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            context_menu.grab_release()

    def show_properties(self):
        """Show properties of selected S3 object"""
        selection = self.s3_tree.selection()
        if not selection:
            return

        item = self.s3_tree.item(selection[0])
        item_name = item['values'][0]
        item_type = item['values'][1]

        if item_type == 'Folder':
            messagebox.showinfo("Properties", f"Folder: {item_name}\nType: Directory")
            return

        try:
            bucket = self.bucket_name.get()
            s3_prefix = self.current_path.get().lstrip("/")
            if s3_prefix and not s3_prefix.endswith("/"):
                s3_prefix += "/"

            s3_key = s3_prefix + item_name if s3_prefix else item_name

            # Get object metadata
            response = self.s3_client.head_object(Bucket=bucket, Key=s3_key)

            size = self.format_file_size(response['ContentLength'])
            modified = response['LastModified'].strftime('%Y-%m-%d %H:%M:%S')
            content_type = response.get('ContentType', 'Unknown')
            etag = response['ETag'].strip('"')

            properties = f"""File: {item_name}
Size: {size}
Modified: {modified}
Content Type: {content_type}
ETag: {etag}
S3 Key: {s3_key}"""

            messagebox.showinfo("Properties", properties)

        except Exception as e:
            messagebox.showerror("Error", f"Failed to get properties:\n{str(e)}")

    # Utility Methods
    def format_file_size(self, size_bytes):
        """Format file size in human readable format"""
        if size_bytes == 0:
            return "0 B"

        size_names = ["B", "KB", "MB", "GB", "TB"]
        import math
        i = int(math.floor(math.log(size_bytes, 1024)))
        p = math.pow(1024, i)
        s = round(size_bytes / p, 2)
        return f"{s} {size_names[i]}"

    def get_file_icon(self, filename):
        """Get appropriate icon for file type"""
        if '.' not in filename:
            return "📄"

        ext = filename.lower().split('.')[-1]

        icons = {
            'txt': '📄', 'doc': '📄', 'docx': '📄', 'pdf': '📕',
            'jpg': '🖼️', 'jpeg': '🖼️', 'png': '🖼️', 'gif': '🖼️', 'bmp': '🖼️',
            'mp3': '🎵', 'wav': '🎵', 'flac': '🎵', 'aac': '🎵',
            'mp4': '🎬', 'avi': '🎬', 'mkv': '🎬', 'mov': '🎬',
            'zip': '📦', 'rar': '📦', '7z': '📦', 'tar': '📦',
            'py': '🐍', 'js': '📜', 'html': '🌐', 'css': '🎨',
            'json': '📋', 'xml': '📋', 'csv': '📊', 'xlsx': '📊',
            'exe': '⚙️', 'msi': '⚙️', 'deb': '⚙️', 'rpm': '⚙️'
        }

        return icons.get(ext, '📄')

    def update_status(self, message):
        """Update status label"""
        self.status_label.config(text=message)
        self.root.update_idletasks()


def main():
    """Main function to run the application"""
    root = Tk()
    app = EnhancedS3FileManager(root)

    # Set window icon and properties
    try:
        root.iconphoto(False, PhotoImage(data=""))  # You can add icon data here
    except:
        pass

    # Center window on screen
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f'{width}x{height}+{x}+{y}')

    # Handle window close
    def on_closing():
        if messagebox.askokcancel("Quit", "Do you want to quit?"):
            root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)

    # Start the application
    root.mainloop()


if __name__ == "__main__":
    main()
