# JobStreet/JobsDB Scraper with GUI Interface
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox, filedialog
import threading
import asyncio
import sys
import os
from datetime import datetime
import json
from pathlib import Path

# Import your original scraper (make sure it's in the same directory)
try:
    from job_street_scraper import web_scraper
except ImportError:
    print("Warning: job_street_scraper.py not found. Running in demo mode.")
    web_scraper = None


class JobScraperGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("JobStreet/JobsDB Scraper v1.0")
        self.root.geometry("900x700")
        self.root.configure(bg='#f0f0f0')

        # Variables
        self.is_running = False
        self.current_thread = None

        # Create GUI elements
        self.create_widgets()

        # Create data directory
        os.makedirs("data", exist_ok=True)

    def create_widgets(self):
        # Main frame
        main_frame = tk.Frame(self.root, bg='#f0f0f0', padx=20, pady=20)
        main_frame.pack(fill='both', expand=True)

        # Title
        title_label = tk.Label(
            main_frame,
            text="🔍 JobStreet/JobsDB Scraper",
            font=('Arial', 20, 'bold'),
            bg='#f0f0f0',
            fg='#2c3e50'
        )
        title_label.pack(pady=(0, 20))

        # Create notebook for tabs
        notebook = ttk.Notebook(main_frame)
        notebook.pack(fill='both', expand=True, pady=(0, 10))

        # Single Job Search Tab
        self.single_tab = ttk.Frame(notebook)
        notebook.add(self.single_tab, text="Single Search")
        self.create_single_search_tab()

        # Batch Test Tab
        self.batch_tab = ttk.Frame(notebook)
        notebook.add(self.batch_tab, text="Batch Testing")
        self.create_batch_test_tab()

        # Settings Tab
        self.settings_tab = ttk.Frame(notebook)
        notebook.add(self.settings_tab, text="Settings")
        self.create_settings_tab()

        # Bottom frame for buttons and status
        bottom_frame = tk.Frame(main_frame, bg='#f0f0f0')
        bottom_frame.pack(fill='x', pady=(10, 0))

        # Status bar
        self.status_var = tk.StringVar(value="Ready to scrape")
        status_label = tk.Label(
            bottom_frame,
            textvariable=self.status_var,
            bg='#e8f5e8',
            fg='#27ae60',
            font=('Arial', 10),
            relief='sunken',
            anchor='w',
            padx=10,
            pady=5
        )
        status_label.pack(fill='x', pady=(10, 0))

    def create_single_search_tab(self):
        # Portal selection frame
        portal_frame = tk.LabelFrame(
            self.single_tab, text="Portal Selection", padx=10, pady=10)
        portal_frame.pack(fill='x', padx=10, pady=5)

        # Portal info
        info_text = """Available Portals:
• ID - Indonesia (JobStreet)
• MY - Malaysia (JobStreet)  
• SG - Singapore (JobStreet)
• PH - Philippines (JobStreet)
• TH - Thailand (JobsDB)"""

        info_label = tk.Label(portal_frame, text=info_text,
                              justify='left', font=('Courier', 9))
        info_label.pack(anchor='w', pady=(0, 10))

        # Portal dropdown
        tk.Label(portal_frame, text="Select Portal:").pack(anchor='w')
        self.portal_var = tk.StringVar()
        portal_combo = ttk.Combobox(
            portal_frame,
            textvariable=self.portal_var,
            values=["id - Indonesia (JobStreet)", "my - Malaysia (JobStreet)",
                    "sg - Singapore (JobStreet)", "ph - Philippines (JobStreet)",
                    "th - Thailand (JobsDB)"],
            state='readonly',
            width=40
        )
        portal_combo.pack(fill='x', pady=(5, 0))

        # Search parameters frame
        params_frame = tk.LabelFrame(
            self.single_tab, text="Search Parameters", padx=10, pady=10)
        params_frame.pack(fill='x', padx=10, pady=5)

        # Location
        tk.Label(params_frame, text="Location (optional):").pack(anchor='w')
        self.location_var = tk.StringVar()
        location_entry = tk.Entry(
            params_frame, textvariable=self.location_var, width=50)
        location_entry.pack(fill='x', pady=(5, 10))

        # Job keyword
        tk.Label(params_frame, text="Job Position/Keyword:").pack(anchor='w')
        self.keyword_var = tk.StringVar(value="Data-Analyst")
        keyword_entry = tk.Entry(
            params_frame, textvariable=self.keyword_var, width=50)
        keyword_entry.pack(fill='x', pady=(5, 10))

        # Number of pages
        tk.Label(params_frame, text="Number of Pages (1-10):").pack(anchor='w')
        self.pages_var = tk.StringVar(value="1")
        pages_frame = tk.Frame(params_frame)
        pages_frame.pack(fill='x', pady=(5, 0))

        pages_spinbox = tk.Spinbox(
            pages_frame,
            from_=1,
            to=10,
            textvariable=self.pages_var,
            width=10
        )
        pages_spinbox.pack(side='left')

        # Control buttons frame
        control_frame = tk.Frame(self.single_tab)
        control_frame.pack(fill='x', padx=10, pady=10)

        # Start button
        self.start_button = tk.Button(
            control_frame,
            text="🚀 Start Scraping",
            command=self.start_scraping,
            bg='#3498db',
            fg='white',
            font=('Arial', 12, 'bold'),
            padx=20,
            pady=10,
            relief='raised'
        )
        self.start_button.pack(side='left', padx=(0, 10))

        # Stop button
        self.stop_button = tk.Button(
            control_frame,
            text="⏹ Stop",
            command=self.stop_scraping,
            bg='#e74c3c',
            fg='white',
            font=('Arial', 12, 'bold'),
            padx=20,
            pady=10,
            relief='raised',
            state='disabled'
        )
        self.stop_button.pack(side='left', padx=(0, 10))

        # Open results button
        self.results_button = tk.Button(
            control_frame,
            text="📂 Open Results",
            command=self.open_results_folder,
            bg='#27ae60',
            fg='white',
            font=('Arial', 12, 'bold'),
            padx=20,
            pady=10,
            relief='raised'
        )
        self.results_button.pack(side='right')

        # Output frame
        output_frame = tk.LabelFrame(
            self.single_tab, text="Output Log", padx=10, pady=10)
        output_frame.pack(fill='both', expand=True, padx=10, pady=5)

        # Output text area
        self.output_text = scrolledtext.ScrolledText(
            output_frame,
            wrap=tk.WORD,
            width=80,
            height=15,
            font=('Courier', 9),
            bg='#2c3e50',
            fg='#ecf0f1'
        )
        self.output_text.pack(fill='both', expand=True)

    def create_batch_test_tab(self):
        # Predefined test configurations
        test_configs = {
            "Quick Test (5 jobs)": [
                {"portal": "ph", "location": "Metro Manila",
                    "keyword": "Data-Analyst", "pages": 1},
                {"portal": "my", "location": "Kuala Lumpur",
                    "keyword": "Software-Engineer", "pages": 1},
            ],
            "Comprehensive Test (All Portals)": [
                {"portal": "ph", "location": "Metro Manila",
                    "keyword": "Data-Analyst", "pages": 1},
                {"portal": "my", "location": "Kuala Lumpur",
                    "keyword": "Software-Engineer", "pages": 1},
                {"portal": "sg", "location": "",
                    "keyword": "Marketing-Manager", "pages": 1},
                {"portal": "th", "location": "Bangkok",
                    "keyword": "Product-Manager", "pages": 1},
                {"portal": "id", "location": "Jakarta",
                    "keyword": "Business-Analyst", "pages": 1},
            ],
            "Job-Specific Test": [
                {"portal": "sg", "location": "",
                    "keyword": "Python-Developer", "pages": 1},
                {"portal": "my", "location": "",
                    "keyword": "Machine-Learning-Engineer", "pages": 1},
                {"portal": "ph", "location": "",
                    "keyword": "Digital-Marketing", "pages": 1},
            ]
        }

        # Test suite selection
        suite_frame = tk.LabelFrame(
            self.batch_tab, text="Test Suite Selection", padx=10, pady=10)
        suite_frame.pack(fill='x', padx=10, pady=5)

        self.suite_var = tk.StringVar()
        for suite_name in test_configs.keys():
            tk.Radiobutton(
                suite_frame,
                text=suite_name,
                variable=self.suite_var,
                value=suite_name,
                font=('Arial', 10)
            ).pack(anchor='w', pady=2)

        # Set default selection
        self.suite_var.set(list(test_configs.keys())[0])

        # Control buttons
        batch_control_frame = tk.Frame(self.batch_tab)
        batch_control_frame.pack(fill='x', padx=10, pady=10)

        self.batch_start_button = tk.Button(
            batch_control_frame,
            text="🧪 Run Test Suite",
            command=lambda: self.start_batch_test(test_configs),
            bg='#9b59b6',
            fg='white',
            font=('Arial', 12, 'bold'),
            padx=20,
            pady=10
        )
        self.batch_start_button.pack(side='left', padx=(0, 10))

        # Results display
        batch_output_frame = tk.LabelFrame(
            self.batch_tab, text="Test Results", padx=10, pady=10)
        batch_output_frame.pack(fill='both', expand=True, padx=10, pady=5)

        self.batch_output_text = scrolledtext.ScrolledText(
            batch_output_frame,
            wrap=tk.WORD,
            font=('Courier', 9),
            bg='#2c3e50',
            fg='#ecf0f1'
        )
        self.batch_output_text.pack(fill='both', expand=True)

    def create_settings_tab(self):
        # Browser settings
        browser_frame = tk.LabelFrame(
            self.settings_tab, text="Browser Settings", padx=10, pady=10)
        browser_frame.pack(fill='x', padx=10, pady=5)

        self.headless_var = tk.BooleanVar(value=False)
        tk.Checkbutton(
            browser_frame,
            text="Run browser in headless mode (background)",
            variable=self.headless_var,
            font=('Arial', 10)
        ).pack(anchor='w')

        # Output settings
        output_settings_frame = tk.LabelFrame(
            self.settings_tab, text="Output Settings", padx=10, pady=10)
        output_settings_frame.pack(fill='x', padx=10, pady=10)

        tk.Label(output_settings_frame,
                 text="Output Directory:").pack(anchor='w')
        output_dir_frame = tk.Frame(output_settings_frame)
        output_dir_frame.pack(fill='x', pady=(5, 10))

        self.output_dir_var = tk.StringVar(value="data")
        output_dir_entry = tk.Entry(
            output_dir_frame, textvariable=self.output_dir_var, width=50)
        output_dir_entry.pack(side='left', fill='x', expand=True, padx=(0, 10))

        browse_button = tk.Button(
            output_dir_frame,
            text="Browse",
            command=self.browse_output_dir,
            bg='#95a5a6',
            fg='white'
        )
        browse_button.pack(side='right')

        # About section
        about_frame = tk.LabelFrame(
            self.settings_tab, text="About", padx=10, pady=10)
        about_frame.pack(fill='both', expand=True, padx=10, pady=5)

        about_text = """JobStreet/JobsDB Web Scraper v1.0

This application scrapes job listings from JobStreet and JobsDB portals 
across Southeast Asian countries.

Features:
• Support for multiple portals (ID, MY, SG, PH, TH)
• Batch testing capabilities
• Real-time progress monitoring
• CSV export functionality
• Company information extraction

Requirements:
• Python 3.7+
• playwright
• pandas
• asyncio

For support and updates, check the documentation."""

        about_label = tk.Label(
            about_frame,
            text=about_text,
            justify='left',
            font=('Arial', 9),
            wraplength=400
        )
        about_label.pack(anchor='w', pady=10)

    def log_output(self, message, text_widget=None):
        """Add message to output log"""
        if text_widget is None:
            text_widget = self.output_text

        timestamp = datetime.now().strftime("%H:%M:%S")
        formatted_message = f"[{timestamp}] {message}\n"

        text_widget.insert(tk.END, formatted_message)
        text_widget.see(tk.END)
        self.root.update_idletasks()

    def get_portal_code(self):
        """Extract portal code from selected option"""
        portal_selection = self.portal_var.get()
        if portal_selection:
            return portal_selection.split(' -')[0]
        return ""

    def validate_inputs(self):
        """Validate user inputs"""
        if not self.portal_var.get():
            messagebox.showerror("Error", "Please select a portal")
            return False

        if not self.keyword_var.get().strip():
            messagebox.showerror("Error", "Please enter a job keyword")
            return False

        try:
            pages = int(self.pages_var.get())
            if pages < 1 or pages > 10:
                messagebox.showerror("Error", "Pages must be between 1 and 10")
                return False
        except ValueError:
            messagebox.showerror(
                "Error", "Please enter a valid number of pages")
            return False

        return True

    def start_scraping(self):
        """Start the scraping process"""
        if not self.validate_inputs():
            return

        if self.is_running:
            messagebox.showwarning(
                "Warning", "Scraping is already in progress")
            return

        # Clear output
        self.output_text.delete(1.0, tk.END)

        # Update UI state
        self.is_running = True
        self.start_button.config(state='disabled')
        self.stop_button.config(state='normal')
        self.status_var.set("Scraping in progress...")

        # Get parameters
        portal = self.get_portal_code()
        location = self.location_var.get().strip()
        keyword = self.keyword_var.get().strip().replace(' ', '-')
        pages = int(self.pages_var.get())
        site = "jobsdb" if portal == "th" else "jobstreet"

        # Start scraping in separate thread
        self.current_thread = threading.Thread(
            target=self.run_scraper,
            args=(portal, site, location, keyword, pages),
            daemon=True
        )
        self.current_thread.start()

    def run_scraper(self, portal, site, location, keyword, pages):
        """Run the scraper in a separate thread"""
        try:
            self.log_output("🚀 Starting JobStreet/JobsDB Scraper")
            self.log_output(f"Portal: {portal.upper()}")
            self.log_output(f"Site: {site}")
            self.log_output(f"Location: {location or 'Not specified'}")
            self.log_output(f"Keyword: {keyword}")
            self.log_output(f"Pages: {pages}")
            self.log_output("-" * 50)

            if web_scraper is None:
                # Demo mode
                self.log_output(
                    "⚠️  Running in DEMO mode - job_street_scraper.py not found")
                self.log_output("📊 Simulating scraping process...")

                import time
                for i in range(1, pages + 1):
                    if not self.is_running:
                        break
                    self.log_output(f"📄 Processing page {i}/{pages}")
                    time.sleep(2)

                self.log_output("✅ Demo scraping completed!")
                self.log_output(
                    "💡 To enable real scraping, ensure job_street_scraper.py is in the same directory")
            else:
                # Real scraping
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(
                        web_scraper(portal, site, location, keyword, pages)
                    )
                    self.log_output("✅ Scraping completed successfully!")
                finally:
                    loop.close()

        except Exception as e:
            self.log_output(f"❌ Error occurred: {str(e)}")

        finally:
            # Reset UI state
            self.root.after(0, self.reset_ui_state)

    def stop_scraping(self):
        """Stop the scraping process"""
        if self.is_running:
            self.is_running = False
            self.log_output("⏹ Stopping scraper...")
            self.status_var.set("Stopping...")

    def reset_ui_state(self):
        """Reset UI to initial state"""
        self.is_running = False
        self.start_button.config(state='normal')
        self.stop_button.config(state='disabled')
        self.status_var.set("Ready to scrape")

    def start_batch_test(self, test_configs):
        """Start batch testing"""
        if self.is_running:
            messagebox.showwarning(
                "Warning", "Another operation is in progress")
            return

        selected_suite = self.suite_var.get()
        if not selected_suite:
            messagebox.showerror("Error", "Please select a test suite")
            return

        configs = test_configs[selected_suite]

        # Clear output
        self.batch_output_text.delete(1.0, tk.END)

        # Update UI
        self.is_running = True
        self.batch_start_button.config(state='disabled')

        # Start batch test in separate thread
        threading.Thread(
            target=self.run_batch_test,
            args=(selected_suite, configs),
            daemon=True
        ).start()

    def run_batch_test(self, suite_name, configs):
        """Run batch tests"""
        try:
            self.log_output(
                f"🧪 Starting Test Suite: {suite_name}", self.batch_output_text)
            self.log_output(
                f"📊 Total tests: {len(configs)}", self.batch_output_text)
            self.log_output("=" * 60, self.batch_output_text)

            successful_tests = 0

            for i, config in enumerate(configs, 1):
                if not self.is_running:
                    break

                self.log_output(
                    f"\n🔍 Test {i}/{len(configs)}: {config['portal']}-{config['keyword']}", self.batch_output_text)

                try:
                    if web_scraper is None:
                        # Demo mode
                        import time
                        time.sleep(1)  # Simulate work
                        self.log_output("✅ Demo test completed",
                                        self.batch_output_text)
                    else:
                        # Real test
                        site = "jobsdb" if config['portal'] == "th" else "jobstreet"
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        try:
                            loop.run_until_complete(
                                web_scraper(
                                    config['portal'],
                                    site,
                                    config['location'],
                                    config['keyword'],
                                    config['pages']
                                )
                            )
                        finally:
                            loop.close()

                        self.log_output(
                            "✅ Test completed successfully", self.batch_output_text)

                    successful_tests += 1

                except Exception as e:
                    self.log_output(
                        f"❌ Test failed: {str(e)}", self.batch_output_text)

            # Summary
            self.log_output("=" * 60, self.batch_output_text)
            self.log_output(f"📈 Test Summary:", self.batch_output_text)
            self.log_output(
                f"✅ Successful: {successful_tests}/{len(configs)}", self.batch_output_text)
            self.log_output(
                f"❌ Failed: {len(configs) - successful_tests}/{len(configs)}", self.batch_output_text)

        except Exception as e:
            self.log_output(
                f"❌ Batch test error: {str(e)}", self.batch_output_text)

        finally:
            self.root.after(0, self.reset_batch_ui)

    def reset_batch_ui(self):
        """Reset batch test UI"""
        self.is_running = False
        self.batch_start_button.config(state='normal')

    def browse_output_dir(self):
        """Browse for output directory"""
        directory = filedialog.askdirectory(
            initialdir=self.output_dir_var.get())
        if directory:
            self.output_dir_var.set(directory)

    def open_results_folder(self):
        """Open the results folder in file explorer"""
        output_dir = self.output_dir_var.get()
        try:
            if sys.platform == "win32":
                os.startfile(output_dir)
            elif sys.platform == "darwin":
                os.system(f"open '{output_dir}'")
            else:
                os.system(f"xdg-open '{output_dir}'")
        except Exception as e:
            messagebox.showerror("Error", f"Could not open folder: {str(e)}")


def main():
    """Main function to run the GUI application"""
    root = tk.Tk()
    app = JobScraperGUI(root)

    # Center window on screen
    root.update_idletasks()
    width = root.winfo_width()
    height = root.winfo_height()
    x = (root.winfo_screenwidth() // 2) - (width // 2)
    y = (root.winfo_screenheight() // 2) - (height // 2)
    root.geometry(f'{width}x{height}+{x}+{y}')

    # Handle window closing
    def on_closing():
        if app.is_running:
            if messagebox.askokcancel("Quit", "Scraping is in progress. Do you want to quit?"):
                app.is_running = False
                root.destroy()
        else:
            root.destroy()

    root.protocol("WM_DELETE_WINDOW", on_closing)

    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("Application interrupted")


if __name__ == "__main__":
    main()
