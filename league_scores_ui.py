import tkinter as tk
from tkinter import messagebox
from tkinter.ttk import Combobox
from datetime import datetime
import pandas as pd
from utils import ensure_db_exists
import database
import scrape_udisc
import write_to_excel
import create_sql_scripts

def update_league_dropdown():
    """Update the dropdown menu with the list of leagues."""
    leagues = database.fetch_leagues()
    menu = dropdown_league['menu']
    menu.delete(0, 'end')
    for league_id, league_name in leagues:
        menu.add_command(label=league_name, command=lambda value=league_id: on_league_selected(value))

def on_league_selected(league_id):
    """Store the selected league's ID and display the league name."""
    global selected_league_id
    selected_league_id = league_id

    league_name = next((name for id, name in database.fetch_leagues() if id == league_id), "")
    selected_league.set(league_name)
    update_ui_layout()

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

    tk.Label(popup, text="League Cash Percentage:").grid(row=3, column=0, sticky="w")
    entry_popup_cash_percentage = tk.Entry(popup, width=30)
    entry_popup_cash_percentage.grid(row=3, column=1, pady=5)

    tk.Label(popup, text="League Entry Fee:").grid(row=4, column=0, sticky="w")
    entry_popup_entry_fee = tk.Entry(popup, width=30)
    entry_popup_entry_fee.grid(row=4, column=1, pady=5)

    def submit_popup_league():
        league_name = entry_popup_league_name.get()
        is_handicap = var_popup_is_handicap.get()
        league_url = entry_popup_league_url.get()
        cash_percentage = entry_popup_cash_percentage.get()
        entry_fee = entry_popup_entry_fee.get()

        if not league_name:
            messagebox.showerror("Error", "League name cannot be empty.")
            return

        # Check if the league name already exists
        existing_leagues = [name for _, name in database.fetch_leagues()]
        if league_name in existing_leagues:
            messagebox.showerror("Error", "A league with this name already exists.")
            return

        if not cash_percentage.isdigit() or not (0 <= int(cash_percentage) <= 100):
            messagebox.showerror("Error", "Cash percentage must be a number between 0 and 100.")
            return

        if not entry_fee.isdigit() or int(entry_fee) < 0:
            messagebox.showerror("Error", "Entry fee must be a non-negative number.")
            return

        database.create_league(league_name, is_handicap, league_url, int(cash_percentage), int(entry_fee))
        update_league_dropdown()
        popup.destroy()

    tk.Button(popup, text="Add League", command=submit_popup_league).grid(row=5, columnspan=2, pady=10)

def open_edit_league_popup():
    """Open a popup window to edit the selected league."""
    if not selected_league_id:
        messagebox.showerror("Error", "Please select a league to edit.")
        return

    league = database.fetch_league_by_id(selected_league_id)
    if not league:
        messagebox.showerror("Error", "Selected league not found.")
        return

    popup = tk.Toplevel(root)
    popup.title("Edit League")

    tk.Label(popup, text="League Name:").grid(row=0, column=0, sticky="w")
    entry_popup_league_name = tk.Entry(popup, width=30)
    entry_popup_league_name.insert(0, league['name'])
    entry_popup_league_name.grid(row=0, column=1, pady=5)

    tk.Label(popup, text="Is Handicap:").grid(row=1, column=0, sticky="w")
    var_popup_is_handicap = tk.BooleanVar(value=league['is_handicap'])
    checkbox_popup_is_handicap = tk.Checkbutton(popup, variable=var_popup_is_handicap)
    checkbox_popup_is_handicap.grid(row=1, column=1, pady=5, sticky="w")

    tk.Label(popup, text="League URL:").grid(row=2, column=0, sticky="w")
    entry_popup_league_url = tk.Entry(popup, width=30)
    entry_popup_league_url.insert(0, league['url'])
    entry_popup_league_url.grid(row=2, column=1, pady=5)

    tk.Label(popup, text="League Cash Percentage:").grid(row=3, column=0, sticky="w")
    entry_popup_cash_percentage = tk.Entry(popup, width=30)
    entry_popup_cash_percentage.insert(0, league['cash_percentage'])
    entry_popup_cash_percentage.grid(row=3, column=1, pady=5)

    tk.Label(popup, text="League Entry Fee:").grid(row=4, column=0, sticky="w")
    entry_popup_entry_fee = tk.Entry(popup, width=30)
    entry_popup_entry_fee.insert(0, league['entry_fee'])
    entry_popup_entry_fee.grid(row=4, column=1, pady=5)

    def submit_popup_league():
        league_name = entry_popup_league_name.get()
        is_handicap = var_popup_is_handicap.get()
        league_url = entry_popup_league_url.get()
        cash_percentage = entry_popup_cash_percentage.get()
        entry_fee = entry_popup_entry_fee.get()

        if not league_name:
            messagebox.showerror("Error", "League name cannot be empty.")
            return

        if not cash_percentage.isdigit() or not (0 <= int(cash_percentage) <= 100):
            messagebox.showerror("Error", "Cash percentage must be a number between 0 and 100.")
            return

        if not entry_fee.isdigit() or int(entry_fee) < 0:
            messagebox.showerror("Error", "Entry fee must be a non-negative number.")
            return

        database.update_league(selected_league_id, league_name, is_handicap, league_url, int(cash_percentage), int(entry_fee))
        update_league_dropdown()
        popup.destroy()

    tk.Button(popup, text="Save Changes", command=submit_popup_league).grid(row=5, columnspan=2, pady=10)

def open_scrape_scores_popup():
    """Open a popup to scrape scores using the stored league URL."""
    if not selected_league_id:
        messagebox.showerror("Error", "Please select a league first.")
        return

    league_url = database.fetch_league_url(selected_league_id)
    if not league_url:
        messagebox.showerror("Error", "No URL configured for the selected league.")
        return

    popup = tk.Toplevel(root)
    popup.title("Scrape Scores")
    popup.geometry("1000x600")

    tk.Label(popup, text="Select the weeks you want to import from udisc", anchor="w").pack(pady=10, anchor="w")

    tk.Label(popup, text="UDisc Lookback Year:").pack(pady=10)
    year_combobox = Combobox(popup, textvariable=lookback_date, state="readonly")
    year_combobox['values'] = [str(year) for year in range(2000, 2031)]
    year_combobox.pack(pady=5)

    def load_weeks():
        # Clear only the rows of the table, not the headers
        for widget in scrollable_frame.winfo_children():
            if widget != header_frame:  # Preserve the header frame
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

    select_all_var = tk.BooleanVar()
    select_all_checkbox = tk.Checkbutton(popup, text="Select All", variable=select_all_var, command=select_all_weeks)
    select_all_checkbox.pack(pady=5, anchor="w")

    canvas = tk.Canvas(popup)
    scrollbar = tk.Scrollbar(popup, orient="vertical", command=canvas.yview)
    scrollable_frame = tk.Frame(canvas)

    scrollable_frame.bind(
        "<Configure>",
        lambda e: canvas.configure(scrollregion=canvas.bbox("all"))
    )

    canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
    canvas.configure(yscrollcommand=scrollbar.set)

    canvas.pack(side="left", fill="both", expand=True, pady=60)
    scrollbar.pack(side="right", fill="y")

    week_vars = {}
    multiplier_vars = {}
    exclude_vars = {}

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
            database.execute_sql_script('sql/replace_scores.sql')
            database.execute_update_points_script(selected_league_id)
            messagebox.showinfo("Info", f"Weeks imported successfully")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to import weeks: {e}")

    tk.Button(popup, text="Import Selected Weeks", command=import_selected_weeks).pack(pady=10)

def update_ui_layout():
    global dropdown_league

    for widget in frame.winfo_children():
        widget.destroy()

    tk.Label(frame, text="Select League:").grid(row=0, column=0, sticky="w")
    dropdown_league = tk.OptionMenu(frame, selected_league, "", command=lambda _: on_league_selected(selected_league.get()))
    dropdown_league.grid(row=0, column=1, pady=5, sticky="w")
    update_league_dropdown()

    if selected_league.get():
        tk.Button(frame, text="Edit League", command=open_edit_league_popup).grid(row=0, column=2, padx=10, sticky="w")

    tk.Button(frame, text="Add League", command=open_add_league_popup).grid(row=0, column=3, padx=10, sticky="w")

    if selected_league.get():
        tk.Button(frame, text="Get Scores from uDisc", command=open_scrape_scores_popup).grid(row=2, column=0, pady=10, padx=5, sticky="w")
        tk.Button(frame, text="Create Spreadsheet", command=create_spreadsheet).grid(row=4, column=0, pady=10, padx=5, sticky="w")

def create_spreadsheet():
    if not selected_league_id:
        messagebox.showerror("Error", "Please select a league first.")
        return

    try:
        write_to_excel.get_spreadsheet_data(selected_league_id)
        messagebox.showinfo("Success", f"Spreadsheet generated successfully")
    except Exception as e:
        messagebox.showerror("Error", f"Failed to generate spreadsheet: {e}")

ensure_db_exists()
create_sql_scripts.create_sql_files()
database.execute_sql_script('sql/create_scripts.sql')

root = tk.Tk()
root.title("League Management")

lookback_date = tk.StringVar()
lookback_date.set(str(datetime.now().year))

frame = tk.Frame(root, padx=10, pady=10)
frame.pack(padx=10, pady=10)

selected_league = tk.StringVar()
dropdown_league = None

update_ui_layout()

root.mainloop()