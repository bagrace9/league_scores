import tkinter as tk
from tkinter import messagebox
import sqlite3
import os
import scrape_udisc
import pandas as pd
from tkinter.ttk import Combobox
from datetime import datetime
import write_to_excel

def ensure_db_exists():
    """Ensure the SQLite database file exists."""
    if not os.path.exists('league_scores.db'):
        open('league_scores.db', 'w').close()

def connect_to_sqlite():
    """Establish a connection to the SQLite database."""
    try:
        conn = sqlite3.connect('league_scores.db')
        return conn
    except sqlite3.Error as e:
        print(f"Error connecting to SQLite database: {e}")
        return None

def run_create_script():
    """Run the SQL script to create necessary database objects."""
    conn = connect_to_sqlite()
    if conn is None:
        messagebox.showerror("Error", "Failed to connect to the database.")
        return

    try:
        with conn:
            with open('sql/create_scripts.sql', 'r') as sql_file:
                sql_script = sql_file.read()
                conn.executescript(sql_script)
    except Exception as e:
        messagebox.showerror("Error", f"Failed to execute SQL script: {e}")
    finally:
        conn.close()

def create_league(league_name, is_handicap, league_url, cash_percentage):
    conn = connect_to_sqlite()
    if conn is None:
        messagebox.showerror("Error", "Failed to connect to the database.")
        return

    try:
        with conn:
            conn.execute(
                """
                INSERT INTO leagues (league_name, league_is_handicap, league_url, league_cash_percentage)
                VALUES (?, ?, ?, ?)
                """,
                (league_name, is_handicap, league_url, cash_percentage)
            )
        messagebox.showinfo("Success", "League created successfully!")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to create league: {e}")
    finally:
        conn.close()

def fetch_league_url(league_id):
    """Fetch the URL of a league by its ID."""
    conn = connect_to_sqlite()
    if conn is None:
        return None

    try:
        with conn:
            cursor = conn.execute("SELECT league_url FROM leagues WHERE id = ?", (league_id,))
            result = cursor.fetchone()
            return result[0] if result else None
    except Exception as e:
        print(f"Error fetching league URL: {e}")
        return None
    finally:
        conn.close()

def fetch_leagues():
    """Fetch all leagues from the database."""
    conn = connect_to_sqlite()
    if conn is None:
        return []

    try:
        with conn:
            cursor = conn.execute("SELECT id, league_name FROM leagues")
            return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching leagues: {e}")
        return []
    finally:
        conn.close()

def fetch_leagues_with_handicap():
    """Fetch all leagues with handicap information from the database."""
    conn = connect_to_sqlite()
    if conn is None:
        return []

    try:
        with conn:
            cursor = conn.execute("SELECT id, league_name, league_is_handicap, league_cash_percentage FROM leagues")
            return cursor.fetchall()
    except Exception as e:
        print(f"Error fetching leagues with handicap: {e}")
        return []
    finally:
        conn.close()

def update_league_dropdown():
    """Update the dropdown menu with the list of leagues."""
    leagues = fetch_leagues()
    menu = dropdown_league['menu']
    menu.delete(0, 'end')
    for league_id, league_name in leagues:
        menu.add_command(label=league_name, command=lambda value=league_id: on_league_selected(value))

def on_league_selected(league_id):
    """Store the selected league's ID and display the league name."""
    global selected_league_id
    selected_league_id = league_id

    # Fetch the league name based on the ID
    league_name = next((name for id, name in fetch_leagues() if id == league_id), "")
    selected_league.set(league_name)
    update_ui_layout()

# Initialize the selected league ID variable
selected_league_id = None

def open_add_league_popup():
    """Open a popup window to add a new league."""
    popup = tk.Toplevel(root)
    popup.title("Add League")

    tk.Label(popup, text="League Name:").grid(row=0, column=0, sticky="w")
    entry_popup_league_name = tk.Entry(popup, width=30)
    entry_popup_league_name.grid(row=0, column=1, pady=5)

    tk.Label(popup, text="Is Handicap:").grid(row=1, column=0, sticky="w")
    var_popup_is_handicap = tk.BooleanVar()
    checkbox_popup_is_handicap = tk.Checkbutton(popup, variable=var_popup_is_handicap)
    checkbox_popup_is_handicap.grid(row=1, column=1, pady=5, sticky="w")

    tk.Label(popup, text="League URL:").grid(row=2, column=0, sticky="w")
    entry_popup_league_url = tk.Entry(popup, width=30)
    entry_popup_league_url.grid(row=2, column=1, pady=5)

    tk.Label(popup, text="Cash Percentage:").grid(row=3, column=0, sticky="w")
    entry_popup_cash_percentage = tk.Entry(popup, width=30)
    entry_popup_cash_percentage.grid(row=3, column=1, pady=5)

    def submit_popup_league():
        league_name = entry_popup_league_name.get()
        is_handicap = var_popup_is_handicap.get()
        league_url = entry_popup_league_url.get()
        cash_percentage = entry_popup_cash_percentage.get()

        if not league_name:
            messagebox.showerror("Error", "League name cannot be empty.")
            return

        if not cash_percentage.isdigit() or not (0 <= int(cash_percentage) <= 100):
            messagebox.showerror("Error", "Cash percentage must be a number between 0 and 100.")
            return

        create_league(league_name, is_handicap, league_url, int(cash_percentage))
        update_league_dropdown()
        popup.destroy()

    tk.Button(popup, text="Add League", command=submit_popup_league).grid(row=4, columnspan=2, pady=10)

def execute_replace_scores_script():
    """Execute the SQL script to replace scores."""
    try:
        with open('sql/replace_scores.sql', 'r') as sql_file:
            replace_scores_script = sql_file.read()
            conn = connect_to_sqlite()
            if conn:
                try:
                    with conn:
                        conn.executescript(replace_scores_script)
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to execute replace scores script: {e}")
                finally:
                    conn.close()
    except Exception as e:
        messagebox.showerror("Error", f"Failed to read replace scores script: {e}")

def execute_update_points_script(league_id):
    """Execute the SQL script to update points for a specific league."""
    try:
        with open('sql/update_points.sql', 'r') as sql_file:
            update_points_script = sql_file.read()
            update_points_script = update_points_script.replace("{league_id}", str(league_id))
            conn = connect_to_sqlite()
            if conn:
                try:
                    with conn:
                        conn.executescript(update_points_script)
                except Exception as e:
                    messagebox.showerror("Error", f"Failed to execute update points script: {e}")
                finally:
                    conn.close()
    except Exception as e:
        messagebox.showerror("Error", f"Failed to read update points script: {e}")

def open_scrape_scores_popup():
    """Open a popup to scrape scores using the stored league URL."""
    if not selected_league_id:
        messagebox.showerror("Error", "Please select a league first.")
        return

    league_url = fetch_league_url(selected_league_id)
    if not league_url:
        messagebox.showerror("Error", "No URL configured for the selected league.")
        return

    popup = tk.Toplevel(root)
    popup.title("Scrape Scores")

    # Set the initial size of the popup window to be wider
    popup.geometry("1000x600")  # Adjusted width to ensure all elements are visible

    tk.Label(popup, text="Select the weeks you want to import from udisc", anchor="w").pack(pady=10, anchor="w")

    tk.Label(popup, text="UDisc Lookback Year:").pack(pady=10)
    year_combobox = Combobox(popup, textvariable=lookback_date, state="readonly")
    year_combobox['values'] = [str(year) for year in range(2000, 2031)]  # Example range
    year_combobox.pack(pady=5)

    def load_weeks():
        # Clear existing weeks
        for widget in scrollable_frame.winfo_children():
            widget.destroy()

        selected_year = lookback_date.get()
        if not selected_year:
            messagebox.showerror("Error", "Please select a year.")
            return

        try:
            weeks_data = scrape_udisc.get_event_links(league_url, selected_year)
            for i, week_url in enumerate(weeks_data):
                raw_week_name = week_url.split("events/")[1].rsplit("-", 1)[0].strip("-")
                week_name = " ".join(word.capitalize() for word in raw_week_name.split("-"))

                frame = tk.Frame(scrollable_frame)
                frame.pack(anchor="w", pady=2)

                var = tk.BooleanVar()
                checkbox = tk.Checkbutton(frame, variable=var, width=10, anchor="center")
                checkbox.grid(row=i + 1, column=0, padx=5)

                tk.Label(frame, text=week_name, width=60, anchor="w").grid(row=i + 1, column=1, padx=5)

                multiplier_var = tk.IntVar(value=1)
                multiplier_entry = tk.Entry(frame, textvariable=multiplier_var, width=10)
                multiplier_entry.grid(row=i + 1, column=2, padx=5)

                exclude_var = tk.BooleanVar()
                exclude_checkbox = tk.Checkbutton(frame, variable=exclude_var, width=20, anchor="center")
                exclude_checkbox.grid(row=i + 1, column=3, padx=5)

                week_vars[week_url] = var
                multiplier_vars[week_url] = multiplier_var
                exclude_vars[week_url] = exclude_var
        except Exception as e:
            messagebox.showerror("Error", f"Failed to fetch weeks: {e}")

    tk.Button(popup, text="Load Weeks", command=load_weeks).pack(pady=10)

    def select_all_weeks():
        for var in week_vars.values():
            var.set(True)

    # Add a 'Select All' checkbox above the table
    select_all_var = tk.BooleanVar()
    select_all_checkbox = tk.Checkbutton(popup, text="Select All", variable=select_all_var, command=select_all_weeks)
    select_all_checkbox.pack(pady=5, anchor="w")

    # Add a scrollable frame for weeks
    canvas = tk.Canvas(popup)
    scrollbar = tk.Scrollbar(popup, orient="vertical", command=canvas.yview)
    scrollable_frame = tk.Frame(canvas)

    scrollable_frame.bind(
        "<Configure>",
        lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
    )

    canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
    canvas.configure(yscrollcommand=scrollbar.set)

    canvas.pack(side="left", fill="both", expand=True, pady=60)  # Increased padding to move the table further down
    scrollbar.pack(side="right", fill="y")

    week_vars = {}
    multiplier_vars = {}
    exclude_vars = {}

    # Add table headers
    header_frame = tk.Frame(scrollable_frame)
    header_frame.pack(anchor="w", pady=5)
    tk.Label(header_frame, text="Import", width=10, anchor="center").grid(row=0, column=0, padx=5)
    tk.Label(header_frame, text="Week Name", width=60, anchor="center").grid(row=0, column=1, padx=5)
    tk.Label(header_frame, text="Points Multiplier", width=15, anchor="center").grid(row=0, column=2, padx=5)
    tk.Label(header_frame, text="Exclude from Handicap", width=20, anchor="center").grid(row=0, column=3, padx=5)

    def import_selected_weeks():
        selected_weeks = pd.DataFrame([
            (url, var.get(), multiplier_vars[url].get(), exclude_vars[url].get())
            for url, var in week_vars.items() if var.get()
        ], columns=['url', 'selected', 'points_multiplyer', 'handicap_excluded'])
        if selected_weeks.empty:
            messagebox.showerror("Error", "No weeks selected for importing.")
            return

        try:
            scrape_udisc.scrape(selected_weeks, selected_league_id)
            execute_replace_scores_script()
            execute_update_points_script(selected_league_id)
            messagebox.showinfo("Info", f"Weeks imported successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to import weeks: {e}")

    tk.Button(popup, text="Import Selected Weeks", command=import_selected_weeks).pack(pady=10)

def open_update_scores_window():
    """Open a window to scrape and adjust scores for the selected league."""
    if not selected_league.get():
        messagebox.showerror("Error", "Please select a league first.")
        return

    update_window = tk.Toplevel(root)
    update_window.title("Update Scores")

    tk.Label(update_window, text="Scrape and adjust scores for the selected league.").pack(pady=10)

    tk.Button(update_window, text="Scrape Scores", command=open_scrape_scores_popup).pack(pady=5)

def add_edit_league_button():
    """Add an Edit League button to the UI."""
    tk.Button(frame, text="Edit League", command=open_edit_league_popup).grid(row=3, columnspan=2, pady=10)

def open_edit_league_popup():
    """Open a popup to edit the selected league's configuration."""
    if not selected_league_id:
        messagebox.showerror("Error", "Please select a league first.")
        return

    league_url = fetch_league_url(selected_league_id)
    league_data = next((league for league in fetch_leagues_with_handicap() if league[0] == selected_league_id), None)
    league_name = league_data[1] if league_data else ""
    is_handicap = league_data[2] if league_data else False
    cash_percentage = league_data[3] if league_data else 0

    popup = tk.Toplevel(root)
    popup.title("Edit League")

    tk.Label(popup, text="League Name:").grid(row=0, column=0, sticky="w")
    entry_name = tk.Entry(popup, width=30)
    entry_name.insert(0, league_name)
    entry_name.grid(row=0, column=1, pady=5)

    tk.Label(popup, text="League URL:").grid(row=1, column=0, sticky="w")
    entry_url = tk.Entry(popup, width=30)
    entry_url.insert(0, league_url)
    entry_url.grid(row=1, column=1, pady=5)

    tk.Label(popup, text="Is Handicap:").grid(row=2, column=0, sticky="w")
    var_is_handicap = tk.BooleanVar(value=is_handicap)
    checkbox_is_handicap = tk.Checkbutton(popup, variable=var_is_handicap)
    checkbox_is_handicap.grid(row=2, column=1, pady=5, sticky="w")

    tk.Label(popup, text="Cash Percentage:").grid(row=3, column=0, sticky="w")
    entry_cash_percentage = tk.Entry(popup, width=30)
    entry_cash_percentage.insert(0, cash_percentage)
    entry_cash_percentage.grid(row=3, column=1, pady=5)

    def save_changes():
        new_name = entry_name.get()
        new_url = entry_url.get()
        new_is_handicap = var_is_handicap.get()
        new_cash_percentage = entry_cash_percentage.get()

        if not new_cash_percentage.isdigit() or not (0 <= int(new_cash_percentage) <= 100):
            messagebox.showerror("Error", "Cash percentage must be a number between 0 and 100.")
            return

        conn = connect_to_sqlite()
        if conn is None:
            messagebox.showerror("Error", "Failed to connect to the database.")
            return

        try:
            with conn:
                conn.execute(
                    "UPDATE leagues SET league_name = ?, league_url = ?, league_is_handicap = ?, league_cash_percentage = ? WHERE id = ?",
                    (new_name, new_url, new_is_handicap, int(new_cash_percentage), selected_league_id)
                )
            messagebox.showinfo("Success", "League updated successfully!")
            update_league_dropdown()
            popup.destroy()
        except Exception as e:
            messagebox.showerror("Error", f"Failed to update league: {e}")
        finally:
            conn.close()

    tk.Button(popup, text="Save", command=save_changes).grid(row=4, columnspan=2, pady=10)

def update_ui_layout():
    global dropdown_league  # Ensure dropdown_league is accessible

    for widget in frame.winfo_children():
        widget.destroy()

    # League selector on the left
    tk.Label(frame, text="Select League:").grid(row=0, column=0, sticky="w")
    dropdown_league = tk.OptionMenu(frame, selected_league, "", command=lambda _: on_league_selected(selected_league.get()))
    dropdown_league.grid(row=0, column=1, pady=5, sticky="w")
    update_league_dropdown()

    # Edit League button between league selector and Add League button, visible only if a league is selected
    if selected_league.get():
        tk.Button(frame, text="Edit League", command=open_edit_league_popup).grid(row=0, column=2, padx=10, sticky="w")

    # Add League button further to the right
    tk.Button(frame, text="Add League", command=open_add_league_popup).grid(row=0, column=3, padx=10, sticky="w")

    # Show additional UI only if a league is selected
    if selected_league.get():
        # Move the Load Scores button to the left and update its title
        tk.Button(frame, text="Get Scores from uDisc", command=open_scrape_scores_popup).grid(row=2, column=0, pady=10, padx=5, sticky="w")

        # Add a button to create a spreadsheet
        tk.Button(frame, text="Create Spreadsheet", command=create_spreadsheet).grid(row=4, column=0, pady=10, padx=5, sticky="w")

def create_spreadsheet():
    # Generate the spreadsheet directly without opening a new window
    if not selected_league_id:
        messagebox.showerror("Error", "Please select a league first.")
        return

    conn = connect_to_sqlite()
    if conn is None:
        messagebox.showerror("Error", "Failed to connect to the database.")
        return

    try:
        
        write_to_excel.get_spreadsheet_data(selected_league_id)

        messagebox.showinfo("Success", f"Spreadsheet generated successfully")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to generate spreadsheet: {e}")
    finally:
        conn.close()

# Ensure the database exists before running the SQL script
ensure_db_exists()
run_create_script()

# Create the UI
root = tk.Tk()
root.title("League Management")

lookback_date = tk.StringVar()
lookback_date.set(str(datetime.now().year))

frame = tk.Frame(root, padx=10, pady=10)
frame.pack(padx=10, pady=10)

# Ensure dropdown_league is properly initialized
selected_league = tk.StringVar()
dropdown_league = None

# Call the function to update the layout
update_ui_layout()

root.mainloop()