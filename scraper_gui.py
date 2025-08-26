#!/usr/bin/env python3
"""
Web Scraper GUI Interface
A simple GUI interface to run different job board scrapers
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import asyncio
import threading
import os
import sys
from datetime import datetime

# Import the scrapers
try:
    from job_street_scraper import web_scraper as jobstreet_scraper
    from jobnet_scraper import web_scraper as jobnet_scraper
    from vietnamworks_scraper import web_scraper as vietnamworks_scraper
    from careerviet_scraper import web_scraper as careerviet_scraper
except ImportError as e:
    print(f"Error importing scrapers: {e}")
    print("Make sure all scraper files are in the same directory as this interface.")
    sys.exit(1)


class ScraperGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Job Scraper Interface")
        self.root.geometry("600x700")
        self.root.resizable(True, True)

        # Ensure data directory exists
        self.ensure_data_directory()

        # Create main frame
        main_frame = ttk.Frame(root, padding="10")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Configure grid weights
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)
        main_frame.columnconfigure(0, weight=1)

        # Title
        title_label = ttk.Label(main_frame, text="Job Scraper Interface",
                                font=('Arial', 16, 'bold'))
        title_label.grid(row=0, column=0, pady=(0, 20))

        # Create notebook for different scrapers
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.grid(row=1, column=0, sticky=(
            tk.W, tk.E, tk.N, tk.S), pady=(0, 10))
        main_frame.rowconfigure(1, weight=1)

        # Create tabs for each scraper
        self.create_jobstreet_tab()
        self.create_jobnet_tab()
        self.create_vietnamworks_tab()
        self.create_careerviet_tab()

        # Log area
        log_frame = ttk.LabelFrame(
            main_frame, text="Execution Log", padding="5")
        log_frame.grid(row=2, column=0, sticky=(
            tk.W, tk.E, tk.N, tk.S), pady=(10, 0))
        main_frame.rowconfigure(2, weight=1)
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = scrolledtext.ScrolledText(
            log_frame, height=8, width=70)
        self.log_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))

        # Clear log button
        clear_btn = ttk.Button(
            log_frame, text="Clear Log", command=self.clear_log)
        clear_btn.grid(row=1, column=0, pady=(5, 0))

        # Status bar
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        status_bar = ttk.Label(main_frame, textvariable=self.status_var,
                               relief=tk.SUNKEN, anchor=tk.W)
        status_bar.grid(row=3, column=0, sticky=(tk.W, tk.E), pady=(5, 0))

    def ensure_data_directory(self):
        """Ensure the data directory exists"""
        if not os.path.exists('data'):
            os.makedirs('data')

    def log_message(self, message):
        """Add a message to the log"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)
        self.root.update()

    def clear_log(self):
        """Clear the log text"""
        self.log_text.delete(1.0, tk.END)

    def update_status(self, status):
        """Update status bar"""
        self.status_var.set(status)
        self.root.update()

    def create_jobstreet_tab(self):
        """Create JobStreet scraper tab"""
        frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(frame, text="JobStreet/JobsDB")

        # Portal selection
        ttk.Label(frame, text="Portal:").grid(
            row=0, column=0, sticky=tk.W, pady=2)
        self.js_portal_var = tk.StringVar()
        portal_combo = ttk.Combobox(frame, textvariable=self.js_portal_var,
                                    values=["id - Indonesia", "my - Malaysia", "sg - Singapore",
                                            "ph - Philippines", "th - Thailand"],
                                    state="readonly", width=25)
        portal_combo.grid(row=0, column=1, sticky=(
            tk.W, tk.E), padx=(5, 0), pady=2)
        portal_combo.current(0)

        # Location
        ttk.Label(frame, text="Location (optional):").grid(
            row=1, column=0, sticky=tk.W, pady=2)
        self.js_location_var = tk.StringVar()
        ttk.Entry(frame, textvariable=self.js_location_var, width=30).grid(
            row=1, column=1, sticky=(tk.W, tk.E), padx=(5, 0), pady=2)

        # Keyword
        ttk.Label(frame, text="Job Position:").grid(
            row=2, column=0, sticky=tk.W, pady=2)
        self.js_keyword_var = tk.StringVar(value="Data-Analyst")
        ttk.Entry(frame, textvariable=self.js_keyword_var, width=30).grid(
            row=2, column=1, sticky=(tk.W, tk.E), padx=(5, 0), pady=2)

        # Max pages
        ttk.Label(frame, text="Max Pages:").grid(
            row=3, column=0, sticky=tk.W, pady=2)
        self.js_pages_var = tk.StringVar(value="5")
        ttk.Entry(frame, textvariable=self.js_pages_var, width=30).grid(
            row=3, column=1, sticky=(tk.W, tk.E), padx=(5, 0), pady=2)

        # Run button
        self.js_run_btn = ttk.Button(frame, text="Run JobStreet Scraper",
                                     command=self.run_jobstreet)
        self.js_run_btn.grid(row=4, column=0, columnspan=2, pady=20)

        frame.columnconfigure(1, weight=1)

    def create_jobnet_tab(self):
        """Create JobNet scraper tab"""
        frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(frame, text="JobNet")

        # Portal selection
        ttk.Label(frame, text="Portal:").grid(
            row=0, column=0, sticky=tk.W, pady=2)
        self.jn_portal_var = tk.StringVar()
        portal_combo = ttk.Combobox(frame, textvariable=self.jn_portal_var,
                                    values=["mm - Myanmar", "kh - Cambodia"],
                                    state="readonly", width=25)
        portal_combo.grid(row=0, column=1, sticky=(
            tk.W, tk.E), padx=(5, 0), pady=2)
        portal_combo.current(0)

        # Keyword
        ttk.Label(frame, text="Keyword:").grid(
            row=1, column=0, sticky=tk.W, pady=2)
        self.jn_keyword_var = tk.StringVar(value="data+analyst")
        ttk.Entry(frame, textvariable=self.jn_keyword_var, width=30).grid(
            row=1, column=1, sticky=(tk.W, tk.E), padx=(5, 0), pady=2)

        # Max pages
        ttk.Label(frame, text="Max Pages:").grid(
            row=2, column=0, sticky=tk.W, pady=2)
        self.jn_pages_var = tk.StringVar(value="5")
        ttk.Entry(frame, textvariable=self.jn_pages_var, width=30).grid(
            row=2, column=1, sticky=(tk.W, tk.E), padx=(5, 0), pady=2)

        # Run button
        self.jn_run_btn = ttk.Button(frame, text="Run JobNet Scraper",
                                     command=self.run_jobnet)
        self.jn_run_btn.grid(row=3, column=0, columnspan=2, pady=20)

        frame.columnconfigure(1, weight=1)

    def create_vietnamworks_tab(self):
        """Create VietnamWorks scraper tab"""
        frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(frame, text="VietnamWorks")

        # Keyword
        ttk.Label(frame, text="Keyword:").grid(
            row=0, column=0, sticky=tk.W, pady=2)
        self.vw_keyword_var = tk.StringVar(value="data-analyst")
        ttk.Entry(frame, textvariable=self.vw_keyword_var, width=30).grid(
            row=0, column=1, sticky=(tk.W, tk.E), padx=(5, 0), pady=2)

        # Max pages
        ttk.Label(frame, text="Max Pages:").grid(
            row=1, column=0, sticky=tk.W, pady=2)
        self.vw_pages_var = tk.StringVar(value="5")
        ttk.Entry(frame, textvariable=self.vw_pages_var, width=30).grid(
            row=1, column=1, sticky=(tk.W, tk.E), padx=(5, 0), pady=2)

        # Run button
        self.vw_run_btn = ttk.Button(frame, text="Run VietnamWorks Scraper",
                                     command=self.run_vietnamworks)
        self.vw_run_btn.grid(row=2, column=0, columnspan=2, pady=20)

        frame.columnconfigure(1, weight=1)

    def create_careerviet_tab(self):
        """Create CareerViet scraper tab"""
        frame = ttk.Frame(self.notebook, padding="10")
        self.notebook.add(frame, text="CareerViet")

        # Keyword
        ttk.Label(frame, text="Keyword:").grid(
            row=0, column=0, sticky=tk.W, pady=2)
        self.cv_keyword_var = tk.StringVar(value="data-mining")
        ttk.Entry(frame, textvariable=self.cv_keyword_var, width=30).grid(
            row=0, column=1, sticky=(tk.W, tk.E), padx=(5, 0), pady=2)

        # Max pages
        ttk.Label(frame, text="Max Pages:").grid(
            row=1, column=0, sticky=tk.W, pady=2)
        self.cv_pages_var = tk.StringVar(value="5")
        ttk.Entry(frame, textvariable=self.cv_pages_var, width=30).grid(
            row=1, column=1, sticky=(tk.W, tk.E), padx=(5, 0), pady=2)

        # Run button
        self.cv_run_btn = ttk.Button(frame, text="Run CareerViet Scraper",
                                     command=self.run_careerviet)
        self.cv_run_btn.grid(row=2, column=0, columnspan=2, pady=20)

        frame.columnconfigure(1, weight=1)

    def disable_all_buttons(self):
        """Disable all run buttons"""
        self.js_run_btn.config(state='disabled')
        self.jn_run_btn.config(state='disabled')
        self.vw_run_btn.config(state='disabled')
        self.cv_run_btn.config(state='disabled')

    def enable_all_buttons(self):
        """Enable all run buttons"""
        self.js_run_btn.config(state='normal')
        self.jn_run_btn.config(state='normal')
        self.vw_run_btn.config(state='normal')
        self.cv_run_btn.config(state='normal')

    def validate_input(self, pages_str, scraper_name):
        """Validate input parameters"""
        try:
            pages = int(pages_str)
            if pages <= 0:
                messagebox.showerror(
                    "Invalid Input", "Max pages must be a positive number")
                return False
            return True
        except ValueError:
            messagebox.showerror(
                "Invalid Input", "Max pages must be a valid number")
            return False

    def run_scraper_async(self, scraper_func, scraper_name):
        """Run scraper in async context"""
        def run_in_thread():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                loop.run_until_complete(scraper_func())
            finally:
                loop.close()

        thread = threading.Thread(target=run_in_thread)
        thread.daemon = True
        thread.start()

    def run_jobstreet(self):
        """Run JobStreet scraper"""
        if not self.validate_input(self.js_pages_var.get(), "JobStreet"):
            return

        portal = self.js_portal_var.get().split(" - ")[0]
        location = self.js_location_var.get().strip()
        keyword = self.js_keyword_var.get().strip().replace(" ", "-")
        max_pages = int(self.js_pages_var.get())
        site = "jobsdb" if portal == "th" else "jobstreet"

        self.disable_all_buttons()
        self.update_status("Running JobStreet scraper...")
        self.log_message(
            f"Starting JobStreet scraper - Portal: {portal}, Keyword: {keyword}, Pages: {max_pages}")

        async def run_scraper():
            try:
                await jobstreet_scraper(portal=portal, site=site, location=location,
                                        keyword=keyword, max_pages=max_pages)
                self.log_message("JobStreet scraper completed successfully!")
                messagebox.showinfo(
                    "Success", "JobStreet scraper completed successfully!\nCheck the 'data' directory for output files.")
            except Exception as e:
                error_msg = f"JobStreet scraper failed: {str(e)}"
                self.log_message(error_msg)
                messagebox.showerror("Error", error_msg)
            finally:
                self.root.after(0, self.enable_all_buttons)
                self.root.after(0, lambda: self.update_status("Ready"))

        self.run_scraper_async(run_scraper, "JobStreet")

    def run_jobnet(self):
        """Run JobNet scraper"""
        if not self.validate_input(self.jn_pages_var.get(), "JobNet"):
            return

        portal = self.jn_portal_var.get().split(" - ")[0]
        keyword = self.jn_keyword_var.get().strip()
        max_pages = int(self.jn_pages_var.get())

        self.disable_all_buttons()
        self.update_status("Running JobNet scraper...")
        self.log_message(
            f"Starting JobNet scraper - Portal: {portal}, Keyword: {keyword}, Pages: {max_pages}")

        async def run_scraper():
            try:
                await jobnet_scraper(portal=portal, keyword=keyword, max_pages=max_pages)
                self.log_message("JobNet scraper completed successfully!")
                messagebox.showinfo(
                    "Success", "JobNet scraper completed successfully!\nCheck the 'data' directory for output files.")
            except Exception as e:
                error_msg = f"JobNet scraper failed: {str(e)}"
                self.log_message(error_msg)
                messagebox.showerror("Error", error_msg)
            finally:
                self.root.after(0, self.enable_all_buttons)
                self.root.after(0, lambda: self.update_status("Ready"))

        self.run_scraper_async(run_scraper, "JobNet")

    def run_vietnamworks(self):
        """Run VietnamWorks scraper"""
        if not self.validate_input(self.vw_pages_var.get(), "VietnamWorks"):
            return

        keyword = self.vw_keyword_var.get().strip().replace(" ", "-")
        max_pages = int(self.vw_pages_var.get())

        self.disable_all_buttons()
        self.update_status("Running VietnamWorks scraper...")
        self.log_message(
            f"Starting VietnamWorks scraper - Keyword: {keyword}, Pages: {max_pages}")

        async def run_scraper():
            try:
                await vietnamworks_scraper(keyword=keyword, max_pages=max_pages)
                self.log_message(
                    "VietnamWorks scraper completed successfully!")
                messagebox.showinfo(
                    "Success", "VietnamWorks scraper completed successfully!\nCheck the 'data' directory for output files.")
            except Exception as e:
                error_msg = f"VietnamWorks scraper failed: {str(e)}"
                self.log_message(error_msg)
                messagebox.showerror("Error", error_msg)
            finally:
                self.root.after(0, self.enable_all_buttons)
                self.root.after(0, lambda: self.update_status("Ready"))

        self.run_scraper_async(run_scraper, "VietnamWorks")

    def run_careerviet(self):
        """Run CareerViet scraper"""
        if not self.validate_input(self.cv_pages_var.get(), "CareerViet"):
            return

        keyword = self.cv_keyword_var.get().strip().replace(" ", "-")
        max_pages = int(self.cv_pages_var.get())

        self.disable_all_buttons()
        self.update_status("Running CareerViet scraper...")
        self.log_message(
            f"Starting CareerViet scraper - Keyword: {keyword}, Pages: {max_pages}")

        async def run_scraper():
            try:
                await careerviet_scraper(keyword=keyword, max_pages=max_pages)
                self.log_message("CareerViet scraper completed successfully!")
                messagebox.showinfo(
                    "Success", "CareerViet scraper completed successfully!\nCheck the 'data' directory for output files.")
            except Exception as e:
                error_msg = f"CareerViet scraper failed: {str(e)}"
                self.log_message(error_msg)
                messagebox.showerror("Error", error_msg)
            finally:
                self.root.after(0, self.enable_all_buttons)
                self.root.after(0, lambda: self.update_status("Ready"))

        self.run_scraper_async(run_scraper, "CareerViet")


def main():
    """Main function to run the GUI"""
    root = tk.Tk()
    app = ScraperGUI(root)

    try:
        root.mainloop()
    except KeyboardInterrupt:
        print("\nApplication closed by user.")


if __name__ == "__main__":
    main()
