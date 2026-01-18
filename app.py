import streamlit as st
import sqlite3
import pandas as pd
import uuid
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import ast
import json
from datetime import datetime

# --- CONFIGURATION ---
st.set_page_config(page_title="Decarbonization Tool", layout="wide")

DB_PATH = "npv_projects.db"
FUEL_DB_PATH = "fuel_energy.db"
PROJECT_DB_PATH = "co2_calculator.db"  # New DB for Project module


def init_databases():
    # Main NPV/MACC database - UPDATED WITH ALL NEW COLUMNS
    conn = sqlite3.connect(DB_PATH)
    conn.execute('''
    CREATE TABLE IF NOT EXISTS npv_projects (
        id TEXT PRIMARY KEY, 
        organization TEXT, 
        entity_name TEXT,
        unit_name TEXT,
        project_name TEXT,
        base_year TEXT,
        target_year TEXT,
        implementation_date TEXT,
        life_span TEXT,
        project_owner TEXT,
        initiative TEXT, 
        industry TEXT, 
        country TEXT,
        year TEXT, 
        material_energy_data TEXT, 
        option1_data TEXT, 
        option2_data TEXT, 
        result TEXT,
        npv1 REAL, 
        npv2 REAL, 
        mac REAL, 
        total_co2_diff REAL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()
    conn.close()

    # Fuel & Energy database - MAIN ONE FOR THIS MODULE
    conn = sqlite3.connect(FUEL_DB_PATH)

    # Main calculation metadata
    conn.execute('''
        CREATE TABLE IF NOT EXISTS calculations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            unique_code TEXT UNIQUE,
            org_name TEXT,
            sector TEXT,
            baseline_year INTEGER,
            previous_year INTEGER,
            target_year INTEGER,
            baseline_production REAL,
            previous_year_production REAL,
            growth_rate REAL,
            target_production REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Baseline materials/fuels inventory
    conn.execute('''
        CREATE TABLE IF NOT EXISTS materials_baseline (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            calculation_id INTEGER,
            row_num INTEGER,
            scope TEXT,
            name TEXT,
            uom TEXT,
            quantity REAL,
            ef REAL,
            emission REAL,
            energy_factor REAL,
            energy_factor_uom TEXT,
            energy REAL,
            FOREIGN KEY (calculation_id) REFERENCES calculations(id) ON DELETE CASCADE
        )
    ''')

    # Scope-wise reduction percentages
    conn.execute('''
        CREATE TABLE IF NOT EXISTS emission_reductions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            calculation_id INTEGER,
            scope TEXT CHECK(scope IN ('Scope 1', 'Scope 2', 'Scope 3')),
            reduction_pct REAL,
            FOREIGN KEY (calculation_id) REFERENCES calculations(id) ON DELETE CASCADE
        )
    ''')

    # Official baseline year emissions (used when previous ≠ baseline)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS base_value_details (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            calculation_id INTEGER,
            scope TEXT CHECK(scope IN ('Scope 1', 'Scope 2', 'Scope 3')),
            value REAL,
            FOREIGN KEY (calculation_id) REFERENCES calculations(id) ON DELETE CASCADE
        )
    ''')

    conn.commit()
    conn.close()

    # Project module database (unchanged from your original)
    conn = sqlite3.connect(PROJECT_DB_PATH)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_code TEXT UNIQUE,
            organization TEXT,
            entity_name TEXT,
            unit_name TEXT,
            project_name TEXT,
            base_year TEXT,
            target_year TEXT,
            implementation_date TEXT,
            capex TEXT,
            life_span TEXT,
            project_owner TEXT,
            input_data TEXT,
            output_data TEXT,
            costing_data TEXT,
            amp_before REAL,
            amp_after REAL,
            amp_uom TEXT,
            emission_results TEXT,
            costing_results TEXT,
            calculation_method TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.execute('''
        CREATE UNIQUE INDEX IF NOT EXISTS idx_unique_project_name 
        ON projects(project_name)
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS project_actuals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_code TEXT,
            section_type TEXT,
            material_name TEXT,
            row_index INTEGER,
            year_number INTEGER,
            absolute_value REAL,
            specific_value REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (project_code) REFERENCES projects(project_code)
        )
    ''')
    conn.execute('''
        CREATE TABLE IF NOT EXISTS amp_actuals_tracking (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_code TEXT,
            year_number INTEGER,
            amp_value REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (project_code) REFERENCES projects(project_code)
        )
    ''')
    conn.commit()
    conn.close()

init_databases()


def get_saved_macc_projects():
    """
    Load MACC projects from database, properly extracting Annual CO₂e Difference
    from the calculation results.
    """
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Check if new columns exist
        cursor.execute("PRAGMA table_info(npv_projects)")
        columns = [col[1] for col in cursor.fetchall()]

        # Query based on table structure
        if 'project_name' in columns:
            cursor.execute("""
                SELECT id, organization, project_name, initiative, 
                       npv1, npv2, mac, total_co2_diff, result
                FROM npv_projects 
                WHERE mac IS NOT NULL AND total_co2_diff IS NOT NULL
                ORDER BY created_at DESC
            """)
        else:
            cursor.execute("""
                SELECT id, organization, initiative, 
                       npv1, npv2, mac, total_co2_diff, result
                FROM npv_projects 
                WHERE mac IS NOT NULL AND total_co2_diff IS NOT NULL
                ORDER BY created_at DESC
            """)

        projects = []
        for row in cursor.fetchall():
            if len(row) == 9:  # New structure with result field
                pid, org, proj_name, init, npv1, npv2, mac, co2_total, result = row
                display_name = f"{org} - {proj_name}" if org and proj_name else pid
            else:  # Old structure
                pid, org, init, npv1, npv2, mac, co2_total, result = row
                display_name = f"{org} - {init}" if org and init else pid

            # Try to extract Annual CO₂e Difference from result text
            annual_co2_diff = co2_total  # Default to total if we can't parse

            if result:
                # Parse the result text to find "Annual CO₂e Difference"
                import re
                # Look for pattern like "Annual CO₂e Difference: 34,500 tons/year"
                annual_match = re.search(r'Annual CO₂e Difference:\s*([\d,]+\.?\d*)', result)
                if annual_match:
                    try:
                        # Remove commas and convert to float
                        annual_str = annual_match.group(1).replace(',', '')
                        annual_co2_diff = float(annual_str)
                    except:
                        annual_co2_diff = co2_total  # Fallback to total

                # If not found with CO₂e, try alternative patterns
                elif "Annual CO" in result:
                    # Try to find any annual CO2 value
                    lines = result.split('\n')
                    for line in lines:
                        if 'Annual' in line and ('CO₂' in line or 'CO2' in line):
                            numbers = re.findall(r'[\d,]+\.?\d*', line)
                            if numbers:
                                try:
                                    annual_str = numbers[0].replace(',', '')
                                    annual_co2_diff = float(annual_str)
                                    break
                                except:
                                    pass

            net_cost = npv1 - npv2 if npv1 is not None and npv2 is not None else 0

            projects.append({
                'id': pid,
                'name': display_name,
                'mac': mac or 0,
                'co2_reduction': annual_co2_diff or 0,  # Use ANNUAL difference, not total
                'cost': net_cost,
                'total_co2_diff': co2_total or 0  # Keep total for reference if needed
            })

        conn.close()
        return projects

    except Exception as e:
        st.error(f"Error loading projects for dashboard: {e}")
        import traceback
        st.error(traceback.format_exc())
        return []
def save_calculation_to_db(save_data):
    """
    Save the complete Fuel & Energy calculation to the database.
    Properly handles main calculation, baseline materials, reductions, and baseline emissions.

    Returns True on success, False on failure.
    """
    try:
        conn = sqlite3.connect(FUEL_DB_PATH)
        c = conn.cursor()

        # 1. Insert or update the main calculation record
        c.execute('''
            INSERT OR REPLACE INTO calculations 
            (unique_code, org_name, sector, baseline_year, previous_year, target_year,
             baseline_production, previous_year_production, growth_rate, target_production)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            save_data['unique_code'],
            save_data['org_name'],
            save_data['sector'],
            save_data['baseline_year'],
            save_data['previous_year'],
            save_data['target_year'],
            save_data['baseline_production'],
            save_data['previous_year_production'],
            save_data['growth_rate'],
            save_data['target_production']
        ))

        # Get the calculation ID (either newly inserted or existing)
        c.execute("SELECT id FROM calculations WHERE unique_code = ?", (save_data['unique_code'],))
        calc_id = c.fetchone()[0]

        # 2. Delete old baseline material rows (clean replace)
        c.execute("DELETE FROM materials_baseline WHERE calculation_id = ?", (calc_id,))

        # Insert new baseline material rows
        for row_num, row in enumerate(save_data['materials_baseline']):
            c.execute('''
                INSERT INTO materials_baseline 
                (calculation_id, row_num, scope, name, uom, quantity, ef, emission,
                 energy_factor, energy_factor_uom, energy)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                calc_id,
                row_num,
                row['scope'],
                row['name'],
                row['uom'],
                row['quantity'],
                row['ef'],
                row['emission'],
                row['energy_factor'],
                row['energy_uom'],
                row['energy']
            ))

        # 3. Delete and re-insert reduction percentages
        c.execute("DELETE FROM emission_reductions WHERE calculation_id = ?", (calc_id,))
        for scope, pct in save_data['reductions'].items():
            if pct is not None:  # only save if we have value
                c.execute('''
                    INSERT INTO emission_reductions 
                    (calculation_id, scope, reduction_pct)
                    VALUES (?, ?, ?)
                ''', (calc_id, scope, pct))

        # 4. Delete and re-insert baseline emissions (only if not same_year)
        c.execute("DELETE FROM base_value_details WHERE calculation_id = ?", (calc_id,))
        if save_data.get('base_emissions'):  # only when previous_year != baseline_year
            for scope_key, value in save_data['base_emissions'].items():
                if value is not None and value != 0:  # avoid saving zeros unnecessarily
                    c.execute('''
                        INSERT INTO base_value_details 
                        (calculation_id, scope, value)
                        VALUES (?, ?, ?)
                    ''', (calc_id, scope_key, value))

        conn.commit()
        conn.close()
        return True

    except Exception as e:
        if 'conn' in locals():
            conn.rollback()
            conn.close()
        st.error(f"Database save error: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return False
UOM_OPTIONS = ["kg", "tons", "liters", "m³", "kWh", "MWh", "GJ", "MJ", "units", "pieces","SCM","KL","KCal"]
ENERGY_UOM_OPTIONS = ["GJ", "MJ", "kWh", "MWh"]
# --- Fuel & Energy Calculator Functions (unchanged) ---
def load_calculation_from_db(unique_code):
    """
    Load a complete calculation from the database including:
    - Main metadata
    - Baseline materials inventory
    - Reduction percentages
    - Official baseline year emissions (when different from previous year)

    Returns dict with all data or None if not found/error
    """
    try:
        conn = sqlite3.connect(FUEL_DB_PATH)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()

        # 1. Get main calculation record
        c.execute("SELECT * FROM calculations WHERE unique_code = ?", (unique_code,))
        calc_row = c.fetchone()
        if not calc_row:
            conn.close()
            return None

        calc_id = calc_row['id']

        # 2. Load baseline materials
        c.execute("""
            SELECT scope, name, uom, quantity, ef, emission, 
                   energy_factor, energy_factor_uom AS energy_uom, energy, row_num 
            FROM materials_baseline 
            WHERE calculation_id = ? 
            ORDER BY row_num
        """, (calc_id,))
        baseline_rows = [dict(row) for row in c.fetchall()]

        # Make sure quantities and factors are floats
        for row in baseline_rows:
            for key in ['quantity', 'ef', 'emission', 'energy_factor', 'energy']:
                if row[key] is None:
                    row[key] = 0.0
                else:
                    row[key] = float(row[key])

        # 3. Load reduction percentages
        c.execute("""
            SELECT scope, reduction_pct 
            FROM emission_reductions 
            WHERE calculation_id = ?
        """, (calc_id,))

        reductions_pct = {"Scope 1": 0.0, "Scope 2": 0.0, "Scope 3": 0.0}
        for row in c.fetchall():
            scope = row['scope']
            if scope in reductions_pct:
                reductions_pct[scope] = float(row['reduction_pct'] or 0.0) * 100  # convert back to %

        # 4. Load official baseline year emissions (if exist)
        c.execute("""
            SELECT scope, value 
            FROM base_value_details 
            WHERE calculation_id = ?
        """, (calc_id,))

        baseline_input = {"1": 0.0, "2": 0.0, "3": 0.0}
        for row in c.fetchall():
            scope_num = row['scope'].replace("Scope ", "")
            if scope_num in baseline_input:
                baseline_input[scope_num] = float(row['value'] or 0.0)

        conn.close()

        return {
            "meta": dict(calc_row),
            "baseline_rows": baseline_rows,
            "reductions_pct": reductions_pct,
            "baseline_input": baseline_input
        }

    except Exception as e:
        if 'conn' in locals():
            conn.close()
        st.error(f"Load calculation error: {str(e)}")
        import traceback
        st.error(traceback.format_exc())
        return None
def calculate_npv(rate, cashflows):
    rate = float(rate)
    return sum([cf / (1 + rate)**t for t, cf in enumerate(cashflows)])
# --- Full Fuel & Energy Calculator UI (100% unchanged) ---
def fuel_energy_calculator_ui():
    # Compact styling for inventory table rows
    st.markdown(
        """
        <style>
            div.row-widget.stHorizontal {
                margin-top: 0px !important;
                margin-bottom: -12px !important;
                gap: 2px !important;
            }
            .stNumberInput > div > div > div > input,
            .stSelectbox > div > div > div > select {
                padding-top: 2px !important;
                padding-bottom: 2px !important;
                min-height: 32px !important;
            }
            label.st-emotion-cache-1y4kgma {
                margin-bottom: 2px !important;
            }
        </style>
        """,
        unsafe_allow_html=True
    )

    st.header("🌍 Fuel & Energy Emissions Calculator")

    materials_options = [
        "Natural Gas", "Electricity", "Logistics", "Coal", "Diesel",
        "Petrol", "LPG", "Biomass", "Steam", "Other", "Solar", "Wind", "Hybrid", "HSD", "LDO", "Gasoline"
    ]

    # Initialize session state if not present
    if 'calc' not in st.session_state:
        st.session_state.calc = {
            'unique_code': '',
            'org_name': '',
            'sector': '',
            'baseline_year': 2022,
            'target_year': 2030,
            'previous_year': 2023,
            'baseline_production': 1_000.0,
            'previous_year_production': 1_050.0,
            'growth_rate_pct': 5.0,
            'same_year': False,
            'baseline_emissions_input': {"1": 0.0, "2": 0.0, "3": 0.0},
            'baseline_rows': [
                {"scope": "Scope 1", "name": "Natural Gas", "uom": "m³", "quantity": 0.0,
                 "ef": 1.88, "energy_factor": 38.8, "energy_uom": "GJ"},
                {"scope": "Scope 2", "name": "Electricity", "uom": "kWh", "quantity": 0.0,
                 "ef": 0.5, "energy_factor": 0.0036, "energy_uom": "GJ"},
                {"scope": "Scope 3", "name": "Logistics", "uom": "tons", "quantity": 0.0,
                 "ef": 0.1, "energy_factor": 10.0, "energy_uom": "MJ"}
            ],
            'reductions_pct': {"Scope 1": 30.0, "Scope 2": 50.0, "Scope 3": 20.0}
        }

    calc = st.session_state.calc

    # ── Record Management ───────────────────────────────────────────────────────────────
    try:
        conn = sqlite3.connect(FUEL_DB_PATH)
        existing_codes = pd.read_sql_query(
            "SELECT unique_code FROM calculations ORDER BY created_at DESC",
            conn
        )['unique_code'].tolist()
        conn.close()
    except Exception:
        existing_codes = []

    with st.expander("📂 Record Management", expanded=True):
        col1, col2, col3, col4 = st.columns([3, 1, 1, 1])

        selected = col1.selectbox("Load Existing Calculation", [""] + existing_codes)

        if col2.button("Load"):
            if selected:
                loaded = load_calculation_from_db(selected)
                if loaded:
                    meta = loaded['meta']
                    calc.update({
                        'unique_code': meta['unique_code'],
                        'org_name': meta.get('org_name', ''),
                        'sector': meta.get('sector', ''),
                        'baseline_year': meta['baseline_year'],
                        'target_year': meta['target_year'],
                        'previous_year': meta.get('previous_year', meta['baseline_year']),
                        'baseline_production': meta['baseline_production'],
                        'previous_year_production': meta.get('previous_year_production', meta['baseline_production']),
                        'growth_rate_pct': round(meta.get('growth_rate', 0.05) * 100, 2),
                        'same_year': meta['baseline_year'] == meta.get('previous_year', meta['baseline_year']),
                        'baseline_rows': loaded['baseline_rows'],
                        'reductions_pct': loaded['reductions_pct'],
                        'baseline_emissions_input': loaded.get('baseline_input', {"1": 0.0, "2": 0.0, "3": 0.0})
                    })
                    st.success(f"Loaded: **{selected}**")
                    st.rerun()

        if col3.button("Delete"):
            if selected and st.button(f"Confirm Delete {selected}?", type="primary"):
                try:
                    conn = sqlite3.connect(FUEL_DB_PATH)
                    conn.execute("DELETE FROM calculations WHERE unique_code = ?", (selected,))
                    conn.commit()
                    conn.close()
                    st.success("Deleted successfully")
                    st.rerun()
                except Exception as e:
                    st.error(f"Delete failed: {e}")

        if col4.button("🆕 New / Clear"):
            st.session_state.calc = {
                'unique_code': '',
                'org_name': '',
                'sector': '',
                'baseline_year': 2022,
                'target_year': 2030,
                'previous_year': 2023,
                'baseline_production': 1_000.0,
                'previous_year_production': 1_050.0,
                'growth_rate_pct': 5.0,
                'same_year': False,
                'baseline_emissions_input': {"1": 0.0, "2": 0.0, "3": 0.0},
                'baseline_rows': [
                    {"scope": "Scope 1", "name": "Natural Gas", "uom": "m³", "quantity": 0.0,
                     "ef": 1.88, "energy_factor": 38.8, "energy_uom": "GJ"},
                    {"scope": "Scope 2", "name": "Electricity", "uom": "kWh", "quantity": 0.0,
                     "ef": 0.5, "energy_factor": 0.0036, "energy_uom": "GJ"},
                    {"scope": "Scope 3", "name": "Logistics", "uom": "tons", "quantity": 0.0,
                     "ef": 0.1, "energy_factor": 10.0, "energy_uom": "MJ"}
                ],
                'reductions_pct': {"Scope 1": 30.0, "Scope 2": 50.0, "Scope 3": 20.0}
            }
            st.rerun()

    # 1. Organization & Production Forecast
    st.subheader("1. Organization & Production Forecast")

    c1, c2 = st.columns(2)
    calc['org_name'] = c1.text_input("Organization Name", value=calc['org_name'])
    calc['sector'] = c1.text_input("Sector/Industry", value=calc['sector'])

    calc['baseline_year'] = c2.number_input("Baseline Year", 2000, 2100, calc['baseline_year'], 1)
    calc['same_year'] = c2.checkbox("Is Baseline Year the same as Previous Year?", calc['same_year'])

    if not calc['same_year']:
        calc['previous_year'] = c2.number_input("Previous Year", 2000, 2100, calc['previous_year'], 1)
    else:
        calc['previous_year'] = calc['baseline_year']

    calc['target_year'] = c2.number_input("Target Year", calc['baseline_year']+1, 2100, calc['target_year'], 1)

    if c1.button("✨ Auto-Generate ID"):
        if calc['org_name'] and calc['sector']:
            calc['unique_code'] = f"{calc['org_name'][:3].upper()}-{calc['sector'][:3].upper()}-{calc['target_year']}-{uuid.uuid4().hex[:6].upper()}"
            st.rerun()

    st.text_input("Calculation ID", value=calc['unique_code'], disabled=True)

    pc1, pc2, pc3, pc4 = st.columns(4)
    calc['baseline_production'] = pc1.number_input("Baseline Production", value=calc['baseline_production'], step=1000.0, format="%.0f")

    if not calc['same_year']:
        calc['previous_year_production'] = pc2.number_input("Previous Year Production", value=calc['previous_year_production'], step=1000.0, format="%.0f")
    else:
        calc['previous_year_production'] = calc['baseline_production']

    calc['growth_rate_pct'] = pc3.number_input("Growth Rate (%)", value=calc['growth_rate_pct'], step=0.1, min_value=0.0)
    growth_decimal = calc['growth_rate_pct'] / 100
    years = calc['target_year'] - calc['baseline_year']
    target_production = calc['baseline_production'] * ((1 + growth_decimal) ** years)
    pc4.metric("Target Production", f"{target_production:,.0f}")

    # 2. Inventory section
    inventory_header = (
        f"2. Baseline Emissions Inventory (Year {calc['baseline_year']})"
        if calc['same_year'] else
        f"2. Previous Year Inventory (Year {calc['previous_year']}) → Baseline Year: {calc['baseline_year']}"
    )
    st.subheader(inventory_header)

    header_cols = st.columns([1.5, 2.5, 1, 1.5, 1.5, 1.5, 1.2, 1.2, 0.8])
    headers = ["Scope", "Material/Fuel", "UOM", "Quantity", "EF (tCO₂e/unit)", "Emission", "Energy Factor", "En. UOM", " "]

    for col, header_text in zip(header_cols, headers):
        col.markdown(f"**{header_text}**")

    st.markdown("---")

    updated_rows = []
    for i, row in enumerate(calc['baseline_rows']):
        cols = st.columns([1.5, 2.5, 1, 1.5, 1.5, 1.5, 1.2, 1.2, 0.8])

        scope = cols[0].selectbox("Scope", ["Scope 1", "Scope 2", "Scope 3"],
                                  index=["Scope 1", "Scope 2", "Scope 3"].index(row.get('scope', 'Scope 1')),
                                  label_visibility="collapsed", key=f"fuel_scope_{i}")

        name = cols[1].selectbox("Material", materials_options,
                                 index=materials_options.index(row['name']) if row['name'] in materials_options else 0,
                                 label_visibility="collapsed", key=f"fuel_name_{i}")

        uom = cols[2].selectbox("UOM", UOM_OPTIONS,
                                index=UOM_OPTIONS.index(row['uom']) if row['uom'] in UOM_OPTIONS else 0,
                                label_visibility="collapsed", key=f"fuel_uom_{i}")

        qty = cols[3].number_input("", value=float(row.get('quantity', 0.0)), step=0.01,
                                   label_visibility="collapsed", key=f"fuel_qty_{i}")

        ef = cols[4].number_input("", value=float(row.get('ef', 0.0)), format="%.6f", step=0.000001,
                                  label_visibility="collapsed", key=f"fuel_ef_{i}")

        emission = qty * ef
        cols[5].metric("", f"{emission:,.2f}", label_visibility="collapsed")

        en_factor = cols[6].number_input("", value=float(row.get('energy_factor', 0.0)), format="%.4f",
                                         step=0.0001, label_visibility="collapsed", key=f"fuel_enf_{i}")

        en_uom = cols[7].selectbox("", ENERGY_UOM_OPTIONS,
                                   index=ENERGY_UOM_OPTIONS.index(row.get('energy_uom', 'GJ')),
                                   label_visibility="collapsed", key=f"fuel_enu_{i}")

        if cols[8].button("❌", key=f"fuel_del_{i}"):
            calc['baseline_rows'].pop(i)
            st.rerun()

        updated_rows.append({
            "scope": scope, "name": name, "uom": uom, "quantity": qty, "ef": ef,
            "emission": emission, "energy_factor": en_factor, "energy_uom": en_uom,
            "energy": qty * en_factor, "row_num": i
        })

    calc['baseline_rows'] = updated_rows

    if st.button("➕ Add New Row"):
        calc['baseline_rows'].append({
            "scope": "Scope 1", "name": "Other", "uom": "tons",
            "quantity": 0.0, "ef": 0.0, "energy_factor": 0.0,
            "energy_uom": "GJ", "emission": 0.0, "energy": 0.0
        })
        st.rerun()

    # 3. Reduction Targets
    st.subheader("3. Reduction Targets (%)")
    red_cols = st.columns(3)
    calc['reductions_pct']["Scope 1"] = red_cols[0].number_input(
        "Scope 1 (%)", 0.0, 100.0, calc['reductions_pct']["Scope 1"], 1.0)
    calc['reductions_pct']["Scope 2"] = red_cols[1].number_input(
        "Scope 2 (%)", 0.0, 100.0, calc['reductions_pct']["Scope 2"], 1.0)
    calc['reductions_pct']["Scope 3"] = red_cols[2].number_input(
        "Scope 3 (%)", 0.0, 100.0, calc['reductions_pct']["Scope 3"], 1.0)

    # ── 4. Emission Summary ─────────────────────────────────────────────────────────────
    st.subheader("4. Emission Summary (tCO₂e)")

    # Previous emissions
    if calc['baseline_rows']:
        df = pd.DataFrame(calc['baseline_rows'])
        previous_emissions_by_scope = df.groupby('scope')['emission'].sum().reindex(
            ["Scope 1", "Scope 2", "Scope 3"], fill_value=0.0
        )
        total_previous = previous_emissions_by_scope.sum()
    else:
        previous_emissions_by_scope = pd.Series([0.0, 0.0, 0.0], index=["Scope 1", "Scope 2", "Scope 3"])
        total_previous = 0.0

    # Baseline emissions
    if not calc['same_year']:
        st.markdown(f"**Enter Official Baseline Year Emissions for {calc['baseline_year']} (tCO₂e)**")
        b_cols = st.columns(3)
        calc['baseline_emissions_input']["1"] = b_cols[0].number_input(
            f"Scope 1 – Baseline ({calc['baseline_year']})", value=calc['baseline_emissions_input']["1"], step=100.0, format="%.0f")
        calc['baseline_emissions_input']["2"] = b_cols[1].number_input(
            f"Scope 2 – Baseline ({calc['baseline_year']})", value=calc['baseline_emissions_input']["2"], step=100.0, format="%.0f")
        calc['baseline_emissions_input']["3"] = b_cols[2].number_input(
            f"Scope 3 – Baseline ({calc['baseline_year']})", value=calc['baseline_emissions_input']["3"], step=100.0, format="%.0f")
        baseline_emissions_by_scope = pd.Series({
            "Scope 1": calc['baseline_emissions_input']["1"],
            "Scope 2": calc['baseline_emissions_input']["2"],
            "Scope 3": calc['baseline_emissions_input']["3"]
        })
    else:
        baseline_emissions_by_scope = previous_emissions_by_scope.copy()

    total_baseline = baseline_emissions_by_scope.sum()

    # Target emissions
    target_emissions_by_scope = pd.Series(index=["Scope 1", "Scope 2", "Scope 3"], dtype=float)
    for scope in ["Scope 1", "Scope 2", "Scope 3"]:
        baseline_val = baseline_emissions_by_scope.get(scope, 0.0)
        reduction_pct = calc['reductions_pct'][scope] / 100
        target_emissions_by_scope[scope] = baseline_val * (1 - reduction_pct)

    total_target = target_emissions_by_scope.sum()

    # Specific values
    def safe_specific(emission, production):
        return emission / production if production != 0 else 0.0

    previous_specific_by_scope = previous_emissions_by_scope.apply(lambda e: safe_specific(e, calc['previous_year_production']))
    baseline_specific_by_scope = baseline_emissions_by_scope.apply(lambda e: safe_specific(e, calc['baseline_production']))
    target_specific_by_scope = target_emissions_by_scope.apply(lambda e: safe_specific(e, target_production))

    total_previous_specific = safe_specific(total_previous, calc['previous_year_production'])
    total_baseline_specific = safe_specific(total_baseline, calc['baseline_production'])
    total_target_specific = safe_specific(total_target, target_production)

    # BAU calculation
    production_growth_factor = target_production / calc['previous_year_production'] if calc['previous_year_production'] != 0 else 1.0

    bau_emissions_by_scope = previous_emissions_by_scope * production_growth_factor
    bau_specific_by_scope = previous_specific_by_scope.copy()  # intensity unchanged

    total_bau = bau_emissions_by_scope.sum()
    total_bau_specific = safe_specific(total_bau, target_production)

    # Reductions vs BAU
    reduction_quantity_by_scope = bau_emissions_by_scope - target_emissions_by_scope
    total_reduction_quantity = total_bau - total_target

    reduction_sp_by_scope = bau_specific_by_scope - target_specific_by_scope
    total_reduction_sp = total_bau_specific - total_target_specific

    # Build table
    rows = []
    for scope in ["Scope 1", "Scope 2", "Scope 3"]:
        row = {"Scope": scope}
        row["Baseline (Abs)"] = f"{baseline_emissions_by_scope.get(scope, 0):,.0f}"
        row["Baseline (Sp)"] = f"{baseline_specific_by_scope.get(scope, 0):,.3f}"

        if not calc['same_year']:
            row["Previous (Abs)"] = f"{previous_emissions_by_scope.get(scope, 0):,.0f}"
            row["Previous (Sp)"] = f"{previous_specific_by_scope.get(scope, 0):,.3f}"

        row["BAU (Abs)"] = f"{bau_emissions_by_scope[scope]:,.0f}"
        row["BAU (Sp)"] = f"{bau_specific_by_scope[scope]:,.3f}"

        row["Target (Abs)"] = f"{target_emissions_by_scope[scope]:,.0f}"
        row["Target (Sp)"] = f"{target_specific_by_scope[scope]:,.3f}"

        row["Reduction Quantity"] = f"{reduction_quantity_by_scope[scope]:,.0f}"
        row["Reduction (Sp)"] = f"{reduction_sp_by_scope[scope]:,.3f}"

        rows.append(row)

    # Total row
    total_row = {"Scope": "**Total**"}
    total_row["Baseline (Abs)"] = f"**{total_baseline:,.0f}**"
    total_row["Baseline (Sp)"] = f"**{total_baseline_specific:,.3f}**"

    if not calc['same_year']:
        total_row["Previous (Abs)"] = f"**{total_previous:,.0f}**"
        total_row["Previous (Sp)"] = f"**{total_previous_specific:,.3f}**"

    total_row["BAU (Abs)"] = f"**{total_bau:,.0f}**"
    total_row["BAU (Sp)"] = f"**{total_bau_specific:,.3f}**"

    total_row["Target (Abs)"] = f"**{total_target:,.0f}**"
    total_row["Target (Sp)"] = f"**{total_target_specific:,.3f}**"

    total_row["Reduction Quantity"] = f"**{total_reduction_quantity:,.0f}**"
    total_row["Reduction (Sp)"] = f"**{total_reduction_sp:,.3f}**"

    rows.append(total_row)

    summary_df = pd.DataFrame(rows)
    st.table(summary_df.set_index("Scope"))

    # Metrics - showing avoided emissions vs BAU
    m1, m2, m3 = st.columns(3)
    if calc['same_year']:
        m1.metric("Baseline Emissions", f"{total_previous:,.0f} tCO₂e")
        m2.metric(f"Target ({calc['target_year']})", f"{total_target:,.0f} tCO₂e")
        m3.metric("Avoided vs BAU", f"{total_reduction_quantity:,.0f} tCO₂e",
                  delta=f"-{total_reduction_quantity:,.0f}", delta_color="normal")
    else:
        m1.metric(f"Previous ({calc['previous_year']})", f"{total_previous:,.0f} tCO₂e")
        m2.metric(f"Baseline ({calc['baseline_year']})", f"{total_baseline:,.0f} tCO₂e")
        m3.metric(f"Target ({calc['target_year']})", f"{total_target:,.0f} tCO₂e",
                  delta=f"-{total_reduction_quantity:,.0f}", delta_color="normal")

    st.info("**Reduction Quantity** and **Reduction (Sp)** show **avoided emissions** compared to Business-As-Usual (BAU) in the target year.")

    # ── Interactive Graphs ──────────────────────────────────────────────────────────────
    if st.button("📊 Show Visualizations", type="primary"):
        st.markdown("### Visual Comparison – Emissions Pathways")

        # 1. Bar chart: Comparison across scenarios
        fig_bar = go.Figure()

        fig_bar.add_trace(go.Bar(
            name='Baseline',
            x=baseline_emissions_by_scope.index,
            y=baseline_emissions_by_scope.values,
            marker_color='#636EFA'
        ))

        if not calc['same_year']:
            fig_bar.add_trace(go.Bar(
                name='Previous',
                x=previous_emissions_by_scope.index,
                y=previous_emissions_by_scope.values,
                marker_color='#00CC96'
            ))

        fig_bar.add_trace(go.Bar(
            name='BAU (Target Year)',
            x=bau_emissions_by_scope.index,
            y=bau_emissions_by_scope.values,
            marker_color='#FFA15A'
        ))

        fig_bar.add_trace(go.Bar(
            name='Target',
            x=target_emissions_by_scope.index,
            y=target_emissions_by_scope.values,
            marker_color='#EF553B'
        ))

        fig_bar.update_layout(
            title='Emissions by Scope – Baseline vs BAU vs Target',
            barmode='group',
            yaxis_title='tCO₂e',
            xaxis_title='Scope',
            template='plotly_white',
            height=500,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
        )
        st.plotly_chart(fig_bar, use_container_width=True)

        # 2. Waterfall chart – Total reduction path (vs BAU)
        fig_waterfall = go.Figure(go.Waterfall(
            name="Emission Pathway",
            orientation="v",
            measure=["absolute", "relative", "total"],
            x=["BAU (Target)", "Avoided Emissions", "Target"],
            textposition="outside",
            text=[f"{total_bau:,.0f}", f"-{total_reduction_quantity:,.0f}", f"{total_target:,.0f}"],
            y=[total_bau, -total_reduction_quantity, total_target],
            connector={"line": {"color": "rgb(63, 63, 63)"}},
            increasing={"marker": {"color": "#EF553B"}},
            decreasing={"marker": {"color": "#00CC96"}},
            totals={"marker": {"color": "#636EFA"}}
        ))

        fig_waterfall.update_layout(
            title=f"Total Emissions – BAU vs Target ({calc['target_year']})",
            yaxis_title="tCO₂e",
            template='plotly_white',
            height=500
        )
        st.plotly_chart(fig_waterfall, use_container_width=True)

        # Optional 3. Pie chart for Target distribution
        fig_pie = px.pie(
            values=target_emissions_by_scope.values,
            names=target_emissions_by_scope.index,
            title='Target Year Emissions Distribution by Scope',
            color_discrete_sequence=px.colors.qualitative.Plotly,
            hole=0.4
        )
        fig_pie.update_layout(height=450)
        st.plotly_chart(fig_pie, use_container_width=True)

    # Save button
    if st.button("💾 Save Calculation", type="primary", use_container_width=True):
        if not calc['unique_code']:
            st.error("Please generate or enter a Calculation ID first.")
        else:
            base_emissions = {f"Scope {k}": v for k, v in calc['baseline_emissions_input'].items()}
            save_data = {
                'unique_code': calc['unique_code'],
                'org_name': calc['org_name'],
                'sector': calc['sector'],
                'baseline_year': calc['baseline_year'],
                'previous_year': calc['previous_year'],
                'target_year': calc['target_year'],
                'baseline_production': calc['baseline_production'],
                'previous_year_production': calc['previous_year_production'],
                'growth_rate': growth_decimal,
                'target_production': target_production,
                'materials_baseline': calc['baseline_rows'],
                'reductions': {k: v/100 for k, v in calc['reductions_pct'].items()},
                'base_emissions': base_emissions if not calc['same_year'] else None
            }
            if save_calculation_to_db(save_data):
                st.success(f"Successfully saved as **{calc['unique_code']}**")
                st.balloons()
                st.rerun()
def get_materials_from_fuel_energy_db():
    defaults = ["Coal", "Natural Gas", "Electricity", "Diesel", "Gasoline", "LPG", "Biomass",
                "Steam", "Waste", "Renewable Energy", "Carbon Credits", "Logistics"]
    try:
        conn = sqlite3.connect(FUEL_DB_PATH)
        cursor = conn.cursor()

        # Check if table exists and has data
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='materials_baseline'")
        if not cursor.fetchone():
            conn.close()
            return sorted(defaults)

        cursor.execute("SELECT DISTINCT name FROM materials_baseline WHERE name IS NOT NULL AND name != ''")
        baseline = [row[0] for row in cursor.fetchall()]

        # Safely check for materials_target
        try:
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='materials_target'")
            if cursor.fetchone():
                cursor.execute("SELECT DISTINCT name FROM materials_target WHERE name IS NOT NULL AND name != ''")
                target = [row[0] for row in cursor.fetchall()]
            else:
                target = []
        except:
            target = []

        conn.close()
        all_mats = set(defaults + baseline + target)
        return sorted(all_mats)
    except Exception as e:
        st.warning(f"Could not load materials from database (using defaults): {e}")
        return sorted(defaults)
# --- MACC Calculator UI (fully unchanged) ---

def npv_project_analysis_ui():
    st.header("📊 Marginal Abatement Cost Curve (MACC) Calculator")

    # Helper function to get CO2 projects for dropdown
    def get_co2_projects():
        """Get projects from CO2 Project Calculator database"""
        try:
            conn = sqlite3.connect(PROJECT_DB_PATH)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT project_code, organization, entity_name, unit_name, project_name, 
                       base_year, target_year, implementation_date, life_span, project_owner,
                       input_data, output_data, costing_data, amp_before, amp_after, 
                       amp_uom, calculation_method, emission_results
                FROM projects 
                WHERE project_name IS NOT NULL AND project_name != ''
                ORDER BY created_at DESC
            """)
            projects = cursor.fetchall()
            conn.close()

            # Format projects for dropdown
            project_list = []
            for proj in projects:
                display_name = f"{proj[1]} - {proj[3]} - {proj[4]} ({proj[6]})"
                project_list.append({
                    'display': display_name,
                    'code': proj[0],
                    'data': {
                        'organization': proj[1],
                        'entity_name': proj[2],
                        'unit_name': proj[3],
                        'project_name': proj[4],
                        'base_year': proj[5],
                        'target_year': proj[6],
                        'implementation_date': proj[7],
                        'life_span': proj[8],
                        'project_owner': proj[9],
                        'input_data': proj[10],
                        'output_data': proj[11],
                        'costing_data': proj[12],
                        'amp_before': proj[13],
                        'amp_after': proj[14],
                        'amp_uom': proj[15],
                        'calculation_method': proj[16],
                        'emission_results': proj[17]  # Added emission results
                    }
                })
            return project_list
        except Exception as e:
            st.warning(f"Could not load CO2 projects: {e}")
            return []

    # Initialize session state if not exists
    if 'macc' not in st.session_state:
        st.session_state.macc = {
            'project_id': '',
            # General Information fields from CO2 Project Calculator
            'organization': '',
            'entity_name': '',
            'unit_name': '',
            'project_name': '',
            'base_year': '',
            'target_year': '',
            'implementation_date': datetime.today().strftime('%Y-%m-%d'),
            'life_span': '10',
            'project_owner': '',
            # Original MACC fields
            'initiative': '',
            'industry': '',
            'country': '',
            'year': '',
            'reduction': [{'material': '', 'quantity': '', 'uom': 'kg'} for _ in range(3)],
            'addition': [{'material': '', 'quantity': '', 'uom': 'kg'} for _ in range(3)],
            'option1': {
                'label': 'Before Scenario',
                'capex_type': 'Own Investment',
                'capex_own': 0.0,
                'capex_loan_principal': 0.0,
                'capex_loan_interest': 0.0,
                'capex_loan_period': 0,
                'reinvestment': False,
                'reinvestment_year': 0,
                'reinvestment_amount': 0.0,
                'opex_regular_costs': 0.0,
                'opex_fuel_energy_cost': 0.0,
                'inflation_rate': 0.0,
                'fuel_energy_inflation': 0.0,
                'salvage_value': 0.0,
                'residual_value': 0.0,
                'year_of_salvage': 0,
                'annual_benefit': 0.0,
                'benefit_duration': 0,
                'benefit_decline_rate': 0.0,
                'lifetime': 10,
                'discount_rate': 8.0,
                'co2_reduction': 0.0,
                'emission_tracking_period': 10
            },
            'option2': {
                'label': 'After Scenario',
                'capex_type': 'Own Investment',
                'capex_own': 0.0,
                'capex_loan_principal': 0.0,
                'capex_loan_interest': 0.0,
                'capex_loan_period': 0,
                'reinvestment': False,
                'reinvestment_year': 0,
                'reinvestment_amount': 0.0,
                'opex_regular_costs': 0.0,
                'opex_fuel_energy_cost': 0.0,
                'inflation_rate': 0.0,
                'fuel_energy_inflation': 0.0,
                'salvage_value': 0.0,
                'residual_value': 0.0,
                'year_of_salvage': 0,
                'annual_benefit': 0.0,
                'benefit_duration': 0,
                'benefit_decline_rate': 0.0,
                'lifetime': 10,
                'discount_rate': 8.0,
                'co2_reduction': 0.0,
                'emission_tracking_period': 10
            },
            'result': '',
            'calculated_npv1': 0.0,
            'calculated_npv2': 0.0,
            'calculated_mac': 0.0,
            'total_co2_diff': 0.0,
            'selected_co2_project': ''
        }

    macc = st.session_state.macc

    # --- ALWAYS VISIBLE PROJECT MANAGEMENT SECTION ---
    st.markdown("---")
    st.subheader("📁 Project Management")

    # Get saved MACC projects for dropdown
    saved_projects = get_saved_macc_projects()
    # Create options with display names, but store IDs in values
    project_options = [("", "")] + [(p['name'], p['id']) for p in saved_projects]

    col_select, col_display = st.columns([3, 2])

    with col_select:
        # Find current selection index
        current_index = 0
        if macc['project_id']:
            for idx, (name, pid) in enumerate(project_options):
                if pid == macc['project_id']:
                    current_index = idx
                    break

        selected_display = st.selectbox(
            "Select Saved MACC Project",
            options=[name for name, _ in project_options],
            index=current_index,
            key="macc_project_select_main"
        )

        # Get the corresponding project ID from the selected display name
        selected_project = ""
        for name, pid in project_options:
            if name == selected_display:
                selected_project = pid
                break
    with col_display:
        st.text_input("Current Project ID", value=macc['project_id'], disabled=True, key="macc_project_id_main")

    current_project_id = selected_project if selected_project else macc['project_id']

    # Management buttons - ALWAYS VISIBLE
    col_new, col_save, col_load, col_delete = st.columns(4)

    # New Project Button
    if col_new.button("🆕 New Project", key="new_macc_main", use_container_width=True):
        st.session_state.macc = {
            'project_id': '',
            'organization': '',
            'entity_name': '',
            'unit_name': '',
            'project_name': '',
            'base_year': '',
            'target_year': '',
            'implementation_date': datetime.today().strftime('%Y-%m-%d'),
            'life_span': '10',
            'project_owner': '',
            'initiative': '',
            'industry': '',
            'country': '',
            'year': '',
            'reduction': [{'material': '', 'quantity': '', 'uom': 'kg'} for _ in range(3)],
            'addition': [{'material': '', 'quantity': '', 'uom': 'kg'} for _ in range(3)],
            'option1': {
                'label': 'Before Scenario',
                'capex_type': 'Own Investment',
                'capex_own': 0.0,
                'capex_loan_principal': 0.0,
                'capex_loan_interest': 0.0,
                'capex_loan_period': 0,
                'reinvestment': False,
                'reinvestment_year': 0,
                'reinvestment_amount': 0.0,
                'opex_regular_costs': 0.0,
                'opex_fuel_energy_cost': 0.0,
                'inflation_rate': 0.0,
                'fuel_energy_inflation': 0.0,
                'salvage_value': 0.0,
                'residual_value': 0.0,
                'year_of_salvage': 0,
                'annual_benefit': 0.0,
                'benefit_duration': 0,
                'benefit_decline_rate': 0.0,
                'lifetime': 10,
                'discount_rate': 8.0,
                'co2_reduction': 0.0,
                'emission_tracking_period': 10
            },
            'option2': {
                'label': 'After Scenario',
                'capex_type': 'Own Investment',
                'capex_own': 0.0,
                'capex_loan_principal': 0.0,
                'capex_loan_interest': 0.0,
                'capex_loan_period': 0,
                'reinvestment': False,
                'reinvestment_year': 0,
                'reinvestment_amount': 0.0,
                'opex_regular_costs': 0.0,
                'opex_fuel_energy_cost': 0.0,
                'inflation_rate': 0.0,
                'fuel_energy_inflation': 0.0,
                'salvage_value': 0.0,
                'residual_value': 0.0,
                'year_of_salvage': 0,
                'annual_benefit': 0.0,
                'benefit_duration': 0,
                'benefit_decline_rate': 0.0,
                'lifetime': 10,
                'discount_rate': 8.0,
                'co2_reduction': 0.0,
                'emission_tracking_period': 10
            },
            'result': '',
            'calculated_npv1': 0.0,
            'calculated_npv2': 0.0,
            'calculated_mac': 0.0,
            'total_co2_diff': 0.0,
            'selected_co2_project': ''
        }
        st.success("✅ New project created")
        st.rerun()

    # Save Project Button
    if col_save.button("💾 Save Project", key="save_macc_main", use_container_width=True):
        if not macc['project_id']:
            st.error("❌ Please generate a Project ID first (click 'Generate Project ID' button)")
        else:
            try:
                # Prepare all data for saving
                material_data = {'reduction': macc['reduction'], 'addition': macc['addition']}

                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()

                # First, check if the table has the new columns, if not recreate it
                c.execute("PRAGMA table_info(npv_projects)")
                columns = [col[1] for col in c.fetchall()]

                required_columns = ['entity_name', 'unit_name', 'project_name', 'base_year',
                                    'target_year', 'implementation_date', 'life_span', 'project_owner']

                missing_columns = [col for col in required_columns if col not in columns]

                if missing_columns:
                    # Backup old data
                    c.execute("SELECT * FROM npv_projects")
                    old_data = c.fetchall()

                    # Drop old table
                    c.execute("DROP TABLE IF EXISTS npv_projects")

                    # Create new table with all columns
                    c.execute('''
                    CREATE TABLE npv_projects (
                        id TEXT PRIMARY KEY, 
                        organization TEXT, 
                        entity_name TEXT,
                        unit_name TEXT,
                        project_name TEXT,
                        base_year TEXT,
                        target_year TEXT,
                        implementation_date TEXT,
                        life_span TEXT,
                        project_owner TEXT,
                        initiative TEXT, 
                        industry TEXT, 
                        country TEXT,
                        year TEXT, 
                        material_energy_data TEXT, 
                        option1_data TEXT, 
                        option2_data TEXT, 
                        result TEXT,
                        npv1 REAL, 
                        npv2 REAL, 
                        mac REAL, 
                        total_co2_diff REAL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    ''')

                    # Try to restore old data (skip if there's format mismatch)
                    if old_data and len(old_data[0]) == 15:  # Old table had 15 columns
                        for row in old_data:
                            try:
                                c.execute('''
                                    INSERT INTO npv_projects 
                                    (id, organization, initiative, industry, country, year,
                                     material_energy_data, option1_data, option2_data, result,
                                     npv1, npv2, mac, total_co2_diff, created_at)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                ''', row)
                            except:
                                pass  # Skip if there's an error

                # Check if project exists
                c.execute("SELECT id FROM npv_projects WHERE id = ?", (macc['project_id'],))
                existing = c.fetchone()

                if existing:
                    # Update existing project
                    c.execute('''
                        UPDATE npv_projects 
                        SET organization = ?, entity_name = ?, unit_name = ?, project_name = ?,
                            base_year = ?, target_year = ?, implementation_date = ?, life_span = ?, project_owner = ?,
                            initiative = ?, industry = ?, country = ?, year = ?,
                            material_energy_data = ?, option1_data = ?, option2_data = ?, result = ?,
                            npv1 = ?, npv2 = ?, mac = ?, total_co2_diff = ?,
                            created_at = CASE WHEN created_at IS NULL THEN CURRENT_TIMESTAMP ELSE created_at END
                        WHERE id = ?
                    ''', (
                        macc['organization'],
                        macc['entity_name'],
                        macc['unit_name'],
                        macc['project_name'],
                        macc['base_year'],
                        macc['target_year'],
                        macc['implementation_date'],
                        macc['life_span'],
                        macc['project_owner'],
                        macc['initiative'],
                        macc['industry'],
                        macc['country'],
                        macc['year'],
                        str(material_data),
                        str(macc['option1']),
                        str(macc['option2']),
                        macc['result'],
                        macc.get('calculated_npv1', 0.0),
                        macc.get('calculated_npv2', 0.0),
                        macc.get('calculated_mac', 0.0),
                        macc.get('total_co2_diff', 0.0),
                        macc['project_id']
                    ))
                    action = "updated"
                else:
                    # Insert new project
                    c.execute('''
                        INSERT INTO npv_projects 
                        (id, organization, entity_name, unit_name, project_name, 
                         base_year, target_year, implementation_date, life_span, project_owner,
                         initiative, industry, country, year, 
                         material_energy_data, option1_data, option2_data, result,
                         npv1, npv2, mac, total_co2_diff)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        macc['project_id'],
                        macc['organization'],
                        macc['entity_name'],
                        macc['unit_name'],
                        macc['project_name'],
                        macc['base_year'],
                        macc['target_year'],
                        macc['implementation_date'],
                        macc['life_span'],
                        macc['project_owner'],
                        macc['initiative'],
                        macc['industry'],
                        macc['country'],
                        macc['year'],
                        str(material_data),
                        str(macc['option1']),
                        str(macc['option2']),
                        macc['result'],
                        macc.get('calculated_npv1', 0.0),
                        macc.get('calculated_npv2', 0.0),
                        macc.get('calculated_mac', 0.0),
                        macc.get('total_co2_diff', 0.0)
                    ))
                    action = "saved"

                conn.commit()
                conn.close()
                st.success(f"✅ Project {action} successfully: {macc['project_id']}")
                st.rerun()

            except Exception as e:
                st.error(f"❌ Save error: {e}")
                import traceback
                st.error(traceback.format_exc())

    # Load Project Button
    if col_load.button("📂 Load Project", key="load_macc_main", use_container_width=True):
        if not current_project_id or current_project_id == "":
            st.error("❌ Please select a project to load from the dropdown")
        else:
            try:
                conn = sqlite3.connect(DB_PATH)
                c = conn.cursor()
                c.execute("SELECT * FROM npv_projects WHERE id = ?", (current_project_id,))
                row = c.fetchone()

                if row:
                    # Get column names
                    c.execute("PRAGMA table_info(npv_projects)")
                    columns = [col[1] for col in c.fetchall()]
                    row_dict = dict(zip(columns, row))

                    # Parse material data safely
                    mat_data_str = row_dict.get('material_energy_data', '{}')
                    try:
                        mat_data = ast.literal_eval(mat_data_str) if mat_data_str else {}
                    except:
                        mat_data = {}

                    # Parse option data safely
                    option1_str = row_dict.get('option1_data', '{}')
                    option2_str = row_dict.get('option2_data', '{}')
                    try:
                        option1_data = ast.literal_eval(option1_str) if option1_str else {}
                    except:
                        option1_data = {}
                    try:
                        option2_data = ast.literal_eval(option2_str) if option2_str else {}
                    except:
                        option2_data = {}

                    # Ensure reduction and addition arrays have proper structure
                    reduction_data = mat_data.get('reduction', [])
                    addition_data = mat_data.get('addition', [])

                    # Ensure we have exactly 3 items
                    while len(reduction_data) < 3:
                        reduction_data.append({'material': '', 'quantity': '', 'uom': 'kg'})
                    while len(addition_data) < 3:
                        addition_data.append({'material': '', 'quantity': '', 'uom': 'kg'})

                    # Update session state
                    st.session_state.macc.update({
                        'project_id': row_dict.get('id', ''),
                        'organization': row_dict.get('organization', ''),
                        'entity_name': row_dict.get('entity_name', ''),
                        'unit_name': row_dict.get('unit_name', ''),
                        'project_name': row_dict.get('project_name', ''),
                        'base_year': str(row_dict.get('base_year', '')) if row_dict.get('base_year') else '',
                        'target_year': str(row_dict.get('target_year', '')) if row_dict.get('target_year') else '',
                        'implementation_date': row_dict.get('implementation_date',
                                                            datetime.today().strftime('%Y-%m-%d')),
                        'life_span': str(row_dict.get('life_span', '10')) if row_dict.get('life_span') else '10',
                        'project_owner': row_dict.get('project_owner', ''),
                        'initiative': row_dict.get('initiative', ''),
                        'industry': row_dict.get('industry', ''),
                        'country': row_dict.get('country', ''),
                        'year': str(row_dict.get('year', '')) if row_dict.get('year') else '',
                        'reduction': reduction_data[:3],  # Take only first 3
                        'addition': addition_data[:3],  # Take only first 3
                        'option1': option1_data,
                        'option2': option2_data,
                        'result': row_dict.get('result', ''),
                        'calculated_npv1': float(row_dict.get('npv1', 0.0)),
                        'calculated_npv2': float(row_dict.get('npv2', 0.0)),
                        'calculated_mac': float(row_dict.get('mac', 0.0)),
                        'total_co2_diff': float(row_dict.get('total_co2_diff', 0.0))
                    })
                    conn.close()
                    st.success(f"✅ Loaded project: {current_project_id}")
                    st.rerun()
                else:
                    conn.close()
                    st.error("❌ Project not found")
            except Exception as e:
                st.error(f"❌ Load error: {e}")
                import traceback
                st.error(traceback.format_exc())

    # Delete Project Button
    # Delete Project Button with confirmation modal
    if col_delete.button("🗑️ Delete Project", key="delete_macc_main", use_container_width=True):
        if not selected_project or selected_project == "":
            st.error("❌ Please select a project to delete from the dropdown")
        else:
            # Set confirmation state
            st.session_state['delete_confirmation'] = selected_project
            st.rerun()

    # Show confirmation dialog if delete is pending
    if 'delete_confirmation' in st.session_state and st.session_state['delete_confirmation']:
        project_to_delete = st.session_state['delete_confirmation']

        # Get project name for display
        project_name = ""
        for name, pid in project_options:
            if pid == project_to_delete:
                project_name = name
                break

        st.markdown("---")
        st.warning(f"⚠️ Confirm deletion of: **{project_name}**")

        col_confirm1, col_confirm2 = st.columns(2)

        with col_confirm1:
            if st.button("✅ Yes, Delete Permanently", key="confirm_delete_yes", use_container_width=True,
                         type="primary"):
                try:
                    conn = sqlite3.connect(DB_PATH)
                    c = conn.cursor()
                    c.execute("DELETE FROM npv_projects WHERE id = ?", (project_to_delete,))
                    deleted_rows = conn.total_changes
                    conn.commit()
                    conn.close()

                    if deleted_rows > 0:
                        # Clear if current project matches
                        if macc['project_id'] == project_to_delete:
                            # Reset to new project state
                            st.session_state.macc = {
                                'project_id': '',
                                'organization': '',
                                'entity_name': '',
                                'unit_name': '',
                                'project_name': '',
                                'base_year': '',
                                'target_year': '',
                                'implementation_date': datetime.today().strftime('%Y-%m-%d'),
                                'life_span': '10',
                                'project_owner': '',
                                'initiative': '',
                                'industry': '',
                                'country': '',
                                'year': '',
                                'reduction': [{'material': '', 'quantity': '', 'uom': 'kg'} for _ in range(3)],
                                'addition': [{'material': '', 'quantity': '', 'uom': 'kg'} for _ in range(3)],
                                'option1': {
                                    'label': 'Before Scenario',
                                    'capex_type': 'Own Investment',
                                    'capex_own': 0.0,
                                    'capex_loan_principal': 0.0,
                                    'capex_loan_interest': 0.0,
                                    'capex_loan_period': 0,
                                    'reinvestment': False,
                                    'reinvestment_year': 0,
                                    'reinvestment_amount': 0.0,
                                    'opex_regular_costs': 0.0,
                                    'opex_fuel_energy_cost': 0.0,
                                    'inflation_rate': 0.0,
                                    'fuel_energy_inflation': 0.0,
                                    'salvage_value': 0.0,
                                    'residual_value': 0.0,
                                    'year_of_salvage': 0,
                                    'annual_benefit': 0.0,
                                    'benefit_duration': 0,
                                    'benefit_decline_rate': 0.0,
                                    'lifetime': 10,
                                    'discount_rate': 8.0,
                                    'co2_reduction': 0.0,
                                    'emission_tracking_period': 10
                                },
                                'option2': {
                                    'label': 'After Scenario',
                                    'capex_type': 'Own Investment',
                                    'capex_own': 0.0,
                                    'capex_loan_principal': 0.0,
                                    'capex_loan_interest': 0.0,
                                    'capex_loan_period': 0,
                                    'reinvestment': False,
                                    'reinvestment_year': 0,
                                    'reinvestment_amount': 0.0,
                                    'opex_regular_costs': 0.0,
                                    'opex_fuel_energy_cost': 0.0,
                                    'inflation_rate': 0.0,
                                    'fuel_energy_inflation': 0.0,
                                    'salvage_value': 0.0,
                                    'residual_value': 0.0,
                                    'year_of_salvage': 0,
                                    'annual_benefit': 0.0,
                                    'benefit_duration': 0,
                                    'benefit_decline_rate': 0.0,
                                    'lifetime': 10,
                                    'discount_rate': 8.0,
                                    'co2_reduction': 0.0,
                                    'emission_tracking_period': 10
                                },
                                'result': '',
                                'calculated_npv1': 0.0,
                                'calculated_npv2': 0.0,
                                'calculated_mac': 0.0,
                                'total_co2_diff': 0.0,
                                'selected_co2_project': ''
                            }

                        # Clear confirmation state
                        del st.session_state['delete_confirmation']

                        # Force immediate rerun to refresh dropdown
                        st.success(f"✅ Deleted project: {project_name}")
                        st.rerun()
                    else:
                        st.error("❌ Project not found")
                        del st.session_state['delete_confirmation']
                except Exception as e:
                    st.error(f"❌ Delete error: {e}")

        with col_confirm2:
            if st.button("❌ Cancel", key="confirm_delete_no", use_container_width=True):
                # Clear confirmation state
                del st.session_state['delete_confirmation']
                st.rerun()

    st.markdown("---")

    # Create tabs for different sections (CONTENT ONLY)
    tab1, tab2, tab3 = st.tabs(["📋 General Information", "⚙️ Option Parameters (O1 & O2)", "📊 Calculation Results"])

    with tab1:
        st.subheader("General Information")

        # --- CO2 Project Selection ---
        st.markdown("#### Select CO2 Project")
        co2_projects = get_co2_projects()
        co2_options = ["(Select CO2 Project to Auto-fill)"] + [p['display'] for p in co2_projects]

        selected_co2_display = st.selectbox(
            "Choose CO2 Project to import data",
            options=co2_options,
            index=0 if not macc['selected_co2_project'] else next(
                (i + 1 for i, p in enumerate(co2_projects) if p['code'] == macc['selected_co2_project']), 0
            ),
            key="co2_project_select"
        )

        # --- FIXED Auto-fill function with direct costing data extraction ---
        if st.button("Auto-fill from Selected CO2 Project", key="auto_fill_co2"):
            if selected_co2_display != "(Select CO2 Project to Auto-fill)":
                selected_project = next(p for p in co2_projects if p['display'] == selected_co2_display)
                project_data = selected_project['data']

                # Parse JSON data
                import json
                try:
                    input_data = json.loads(project_data['input_data']) if project_data['input_data'] and project_data[
                        'input_data'] != 'null' else []
                    output_data = json.loads(project_data['output_data']) if project_data['output_data'] and \
                                                                             project_data[
                                                                                 'output_data'] != 'null' else []
                    costing_data = json.loads(project_data['costing_data']) if project_data['costing_data'] and \
                                                                               project_data[
                                                                                   'costing_data'] != 'null' else []

                    # Try to get emission results from CO2 project
                    emission_results = {}
                    try:
                        if 'emission_results' in project_data and project_data['emission_results']:
                            emission_results = json.loads(project_data['emission_results'])
                    except:
                        emission_results = {}

                    # Extract material information
                    reduction_materials = []
                    addition_materials = []

                    # Process input data
                    for i, item in enumerate(input_data[:3]):
                        if isinstance(item, dict):
                            material = item.get('material', f'Input {i + 1}')
                            quantity = item.get('quantity', 0.0) or item.get('abs_before', 0.0) or 0.0
                            uom = item.get('uom', 'kg')
                        elif isinstance(item, list) and len(item) > 0:
                            material = item[0] if item[0] else f'Input {i + 1}'
                            quantity = item[3] if len(item) > 3 and item[3] else 0.0
                            uom = item[1] if len(item) > 1 and item[1] else 'kg'
                        else:
                            material = f'Input {i + 1}'
                            quantity = 0.0
                            uom = 'kg'

                        reduction_materials.append({
                            'material': material,
                            'quantity': float(quantity),
                            'uom': uom
                        })

                    # Process output data
                    for i, item in enumerate(output_data[:3]):
                        if isinstance(item, dict):
                            material = item.get('material', f'Output {i + 1}')
                            quantity = item.get('quantity', 0.0) or item.get('abs_after', 0.0) or 0.0
                            uom = item.get('uom', 'kg')
                        elif isinstance(item, list) and len(item) > 0:
                            material = item[0] if item[0] else f'Output {i + 1}'
                            quantity = item[3] if len(item) > 3 and item[3] else 0.0
                            uom = item[1] if len(item) > 1 and item[1] else 'kg'
                        else:
                            material = f'Output {i + 1}'
                            quantity = 0.0
                            uom = 'kg'

                        addition_materials.append({
                            'material': material,
                            'quantity': float(quantity),
                            'uom': uom
                        })

                    # Fill empty slots if we have less than 3 items
                    while len(reduction_materials) < 3:
                        reduction_materials.append({'material': '', 'quantity': '', 'uom': 'kg'})
                    while len(addition_materials) < 3:
                        addition_materials.append({'material': '', 'quantity': '', 'uom': 'kg'})

                    # FIXED COSTING DATA EXTRACTION - SIMPLE AND DIRECT
                    st.write("DEBUG - Raw Costing Data:", costing_data)

                    # Initialize variables
                    capex_value = 0.0
                    opex_regular_before = 0.0
                    opex_regular_after = 0.0
                    opex_fuel_energy_before = 0.0
                    opex_fuel_energy_after = 0.0

                    # DIRECT EXTRACTION BASED ON KNOWN STRUCTURE
                    # In CO2 Project Calculator, costing_data is a list of dictionaries with specific structure
                    for idx, item in enumerate(costing_data):
                        if isinstance(item, dict):
                            # Get material name (case insensitive)
                            material_name = str(item.get('material', '')).lower()

                            # Get values - try multiple possible field names
                            abs_before = 0.0
                            abs_after = 0.0
                            spec_before = 0.0
                            spec_after = 0.0

                            # Try all possible field names
                            for field in ['abs_before', 'Abs Actual-Before', 'abs_before_value', 'before_value']:
                                if field in item:
                                    try:
                                        abs_before = float(item[field])
                                        break
                                    except:
                                        continue

                            for field in ['abs_after', 'Abs Planned-After', 'abs_after_value', 'after_value']:
                                if field in item:
                                    try:
                                        abs_after = float(item[field])
                                        break
                                    except:
                                        continue

                            for field in ['spec_before', 'Spec Actual-Before', 'spec_before_value', 'before_spec']:
                                if field in item:
                                    try:
                                        spec_before = float(item[field])
                                        break
                                    except:
                                        continue

                            for field in ['spec_after', 'Spec Planned-After', 'spec_after_value', 'after_spec']:
                                if field in item:
                                    try:
                                        spec_after = float(item[field])
                                        break
                                    except:
                                        continue

                            # Determine which value to use based on calculation method
                            is_absolute = project_data.get('calculation_method', 'absolute') == 'absolute'

                            if is_absolute:
                                before_val = abs_before
                                after_val = abs_after
                            else:
                                before_val = spec_before
                                after_val = spec_after

                            st.write(
                                f"DEBUG - Item {idx}: material={material_name}, before={before_val}, after={after_val}")

                            # Match by material name patterns - CORRECTED LOGIC HERE
                            if 'capex' in material_name:
                                capex_value = after_val if after_val != 0 else before_val
                                st.write(f"DEBUG - Found CAPEX: {capex_value}")

                            # Check for "Other/Regular" BEFORE checking for "Fuel/Energy" to prevent overlap
                            elif 'opex' in material_name and ('other' in material_name or 'non-fuel' in material_name or 'regular' in material_name):
                                opex_regular_before = before_val
                                opex_regular_after = after_val
                                st.write(
                                    f"DEBUG - Found OPEX-Other than Fuel/Energy: Before={opex_regular_before}, After={opex_regular_after}")

                            elif 'opex' in material_name and ('fuel' in material_name or 'energy' in material_name):
                                opex_fuel_energy_before = before_val
                                opex_fuel_energy_after = after_val
                                st.write(
                                    f"DEBUG - Found OPEX-Only Fuel/Energy: Before={opex_fuel_energy_before}, After={opex_fuel_energy_after}")

                    # If still zero, try position-based extraction (fallback)
                    if opex_regular_before == 0 and opex_fuel_energy_before == 0 and len(costing_data) >= 3:
                        st.write("DEBUG - Using position-based fallback")

                        # Try to extract by position (common structure)
                        try:
                            # Position 0: CAPEX
                            item0 = costing_data[0]
                            if isinstance(item0, dict):
                                capex_value = float(
                                    item0.get('abs_after', 0) or item0.get('abs_before', 0) or item0.get('spec_after',
                                                                                                         0) or item0.get(
                                        'spec_before', 0) or 0)

                            # Position 1: OPEX-Only Fuel/Energy
                            if len(costing_data) > 1:
                                item1 = costing_data[1]
                                if isinstance(item1, dict):
                                    opex_fuel_energy_before = float(
                                        item1.get('abs_before', 0) or item1.get('spec_before', 0) or 0)
                                    opex_fuel_energy_after = float(
                                        item1.get('abs_after', 0) or item1.get('spec_after', 0) or 0)

                            # Position 2: OPEX-Other than Fuel/Energy
                            if len(costing_data) > 2:
                                item2 = costing_data[2]
                                if isinstance(item2, dict):
                                    opex_regular_before = float(
                                        item2.get('abs_before', 0) or item2.get('spec_before', 0) or 0)
                                    opex_regular_after = float(
                                        item2.get('abs_after', 0) or item2.get('spec_after', 0) or 0)
                        except:
                            pass

                    # Get Life Span from CO2 project
                    life_span_str = project_data.get('life_span', '10')
                    try:
                        life_span = int(life_span_str) if life_span_str and life_span_str.isdigit() else 10
                    except:
                        life_span = 10

                    # Calculate CO2 reduction
                    co2_reduction_value = 0.0
                    net_co2_before = 0.0
                    net_co2_after = 0.0

                    # Method 1: Try to get from emission results
                    try:
                        if emission_results:
                            if 'CO2 reduction_Net' in emission_results:
                                co2_reduction_value = float(emission_results.get('CO2 reduction_Net', 0))
                            elif 'CO2 reduction' in emission_results:
                                co2_reduction_value = float(emission_results.get('CO2 reduction', 0))

                            if 'Net CO2_Before' in emission_results:
                                net_co2_before = float(emission_results.get('Net CO2_Before', 0))
                            if 'Net CO2_After' in emission_results:
                                net_co2_after = float(emission_results.get('Net CO2_After', 0))

                            if co2_reduction_value <= 0 and net_co2_before > 0 and net_co2_after > 0:
                                co2_reduction_value = net_co2_before - net_co2_after
                    except:
                        co2_reduction_value = 0.0
                        net_co2_before = 0.0
                        net_co2_after = 0.0

                    # Method 2: Fallback calculation
                    if co2_reduction_value <= 0:
                        try:
                            amp_before = float(project_data['amp_before'] or 0)
                            amp_after = float(project_data['amp_after'] or 0)
                            co2_reduction_value = (amp_before - amp_after) * 0.8
                            if co2_reduction_value < 0:
                                co2_reduction_value = 0
                        except:
                            co2_reduction_value = 1000

                    # Ensure positive value
                    co2_reduction_value = abs(co2_reduction_value)

                    # Calculate CO₂e Reduction values
                    co2_reduction_target = net_co2_before if net_co2_before > 0 else co2_reduction_value * 1.2
                    co2_reduction_achieved = net_co2_after if net_co2_after > 0 else co2_reduction_value

                    # Update MACC session state
                    st.session_state.macc.update({
                        'selected_co2_project': selected_project['code'],
                        'organization': project_data['organization'] or '',
                        'entity_name': project_data['entity_name'] or '',
                        'unit_name': project_data['unit_name'] or '',
                        'project_name': project_data['project_name'] or '',
                        'base_year': str(project_data['base_year']) if project_data['base_year'] else '',
                        'target_year': str(project_data['target_year']) if project_data['target_year'] else '',
                        'implementation_date': project_data['implementation_date'] or datetime.today().strftime(
                            '%Y-%m-%d'),
                        'life_span': str(life_span),
                        'project_owner': project_data['project_owner'] or '',
                        'initiative': project_data['project_name'] or '',
                        'reduction': reduction_materials,
                        'addition': addition_materials,
                        'option1': {
                            'label': 'Before Scenario',
                            'capex_type': 'Own Investment',
                            'capex_own': float(capex_value),
                            'capex_loan_principal': 0.0,
                            'capex_loan_interest': 0.0,
                            'capex_loan_period': 0,
                            'reinvestment': False,
                            'reinvestment_year': 0,
                            'reinvestment_amount': 0.0,
                            'opex_regular_costs': float(opex_regular_before),
                            'opex_fuel_energy_cost': float(opex_fuel_energy_before),
                            'inflation_rate': 0.0,
                            'fuel_energy_inflation': 0.0,
                            'salvage_value': 0.0,
                            'residual_value': 0.0,
                            'year_of_salvage': 0,
                            'annual_benefit': 0.0,
                            'benefit_duration': 0,
                            'benefit_decline_rate': 0.0,
                            'lifetime': int(life_span),
                            'discount_rate': 8.0,
                            'co2_reduction': float(co2_reduction_target),
                            'emission_tracking_period': int(life_span)
                        },
                        'option2': {
                            'label': 'After Scenario',
                            'capex_type': 'Own Investment',
                            'capex_own': float(capex_value * 0.9),
                            'capex_loan_principal': 0.0,
                            'capex_loan_interest': 0.0,
                            'capex_loan_period': 0,
                            'reinvestment': False,
                            'reinvestment_year': 0,
                            'reinvestment_amount': 0.0,
                            'opex_regular_costs': float(opex_regular_after),
                            'opex_fuel_energy_cost': float(opex_fuel_energy_after),
                            'inflation_rate': 0.0,
                            'fuel_energy_inflation': 0.0,
                            'salvage_value': 0.0,
                            'residual_value': 0.0,
                            'year_of_salvage': 0,
                            'annual_benefit': 0.0,
                            'benefit_duration': 0,
                            'benefit_decline_rate': 0.0,
                            'lifetime': int(life_span),
                            'discount_rate': 8.0,
                            'co2_reduction': float(co2_reduction_achieved),
                            'emission_tracking_period': int(life_span)
                        }
                    })

                    st.success(f"✅ Auto-filled data from: {selected_project['display']}")

                    # Show extraction results
                    extraction_details = f"""
        **Extracted values from CO2 Project:**
        - **CAPEX (Own Investment):** ₹{capex_value:,.2f}
        - **OPEX-Other than Fuel/Energy (Before):** ₹{opex_regular_before:,.2f}
        - **OPEX-Other than Fuel/Energy (After):** ₹{opex_regular_after:,.2f}
        - **OPEX-Only Fuel/Energy (Before):** ₹{opex_fuel_energy_before:,.2f}
        - **OPEX-Only Fuel/Energy (After):** ₹{opex_fuel_energy_after:,.2f}
        - **Project Lifetime:** {life_span} years
        - **Emission Tracking Period:** {life_span} years
        - **Net CO2 Before:** {net_co2_before:,.2f} tons
        - **Net CO2 After:** {net_co2_after:,.2f} tons
        - **CO₂e Reduction Target (Before Scenario):** {co2_reduction_target:,.2f} tons
        - **CO₂e Reduction Achieved (After Scenario):** {co2_reduction_achieved:,.2f} tons
        """

                    if emission_results:
                        extraction_details += f"\n**From Emission Results:**"
                        for key, value in emission_results.items():
                            if 'CO2' in key or 'Net' in key:
                                extraction_details += f"\n- {key}: {value}"

                    st.info(extraction_details)
                    st.session_state.macc['life_span'] = str(life_span)
                    st.rerun()

                except Exception as e:
                    st.error(f"Error parsing project data: {e}")
                    import traceback
                    st.error(traceback.format_exc())
            else:
                st.warning("Please select a CO2 project first")
        st.markdown("---")

        # --- General Information Fields ---
        st.markdown("#### Project Details")
        g1, g2, g3, g4, g5 = st.columns(5)

        with g1:
            st.markdown("**Organization**")
            macc['organization'] = st.text_input("Org", value=macc['organization'],
                                                 label_visibility="collapsed", key="macc_org_tab1")
            st.markdown("**Entity Name**")
            macc['entity_name'] = st.text_input("Entity", value=macc['entity_name'],
                                                label_visibility="collapsed", key="macc_entity_tab1")

        with g2:
            st.markdown("**Unit Name**")
            macc['unit_name'] = st.text_input("Unit", value=macc['unit_name'],
                                              label_visibility="collapsed", key="macc_unit_tab1")
            st.markdown("**Project Name**")
            macc['project_name'] = st.text_input("Proj Name", value=macc['project_name'],
                                                 label_visibility="collapsed", key="macc_proj_name_tab1")

        with g3:
            st.markdown("**Base Year**")
            macc['base_year'] = st.text_input("Base Yr", value=macc['base_year'],
                                              label_visibility="collapsed", key="macc_base_year_tab1")
            st.markdown("**Target Year**")
            macc['target_year'] = st.text_input("Target Yr", value=macc['target_year'],
                                                label_visibility="collapsed", key="macc_target_year_tab1")

        with g4:
            st.markdown("**Implementation Date**")
            if macc['implementation_date']:
                try:
                    date_obj = datetime.strptime(macc['implementation_date'], '%Y-%m-%d')
                except:
                    date_obj = datetime.today()
            else:
                date_obj = datetime.today()

            selected_date = st.date_input(
                "Impl Date",
                value=date_obj,
                label_visibility="collapsed",
                key="macc_impl_date_tab1"
            )
            macc['implementation_date'] = selected_date.strftime('%Y-%m-%d')

            st.markdown("**Life Span (Years)**")
            macc['life_span'] = st.text_input("Life Span", value=macc['life_span'],
                                              label_visibility="collapsed", key="macc_life_span_tab1")

        with g5:
            st.markdown("**Project Owner**")
            macc['project_owner'] = st.text_input("Owner", value=macc['project_owner'],
                                                  label_visibility="collapsed", key="macc_owner_tab1")
            st.markdown("**Initiative/Project Name**")
            macc['initiative'] = st.text_input("Initiative", value=macc['initiative'],
                                               label_visibility="collapsed", key="macc_initiative_tab1")

        # Industry and Country
        col1, col2 = st.columns(2)
        macc['industry'] = col1.text_input("Industry/Sector", value=macc['industry'], key="macc_industry_tab1")
        macc['country'] = col2.text_input("Country", value=macc['country'], key="macc_country_tab1")

        # Generate Project ID Button (in Tab 1)
        if st.button("Generate Project ID", key="gen_macc_id_tab1"):
            if macc['organization'] and macc['project_name']:
                org_code = macc['organization'][:3].upper() if macc['organization'] else "ORG"
                entity_code = macc['entity_name'][:2].upper() if macc['entity_name'] else "EN"
                unit_code = macc['unit_name'][:2].upper() if macc['unit_name'] else "UN"
                project_code = macc['project_name'][:3].upper() if macc['project_name'] else "PRJ"
                owner_code = macc['project_owner'][:2].upper() if macc['project_owner'] else "PO"
                year_code = macc['target_year'] if macc['target_year'] else str(datetime.today().year)

                base_id = f"MACC-{org_code}-{entity_code}-{unit_code}-{project_code}-{year_code}-{owner_code}"
                unique_suffix = uuid.uuid4().hex[:4].upper()
                macc['project_id'] = f"{base_id}-{unique_suffix}"
                st.success(f"Generated Project ID: {macc['project_id']}")
                st.rerun()
            else:
                st.error("Organization and Project Name are required to generate ID")

        st.markdown("---")

        # Material/Energy Changes
        st.markdown("#### Material/Energy Changes")

        st.markdown("**Materials/Energy Reduced (Before Scenario)**")
        for i in range(3):
            cols = st.columns(3)
            # Safely get values with defaults
            material_val = macc['reduction'][i]['material'] if i < len(macc['reduction']) and 'material' in \
                                                               macc['reduction'][i] else ''
            quantity_val = macc['reduction'][i]['quantity'] if i < len(macc['reduction']) and 'quantity' in \
                                                               macc['reduction'][i] else ''
            uom_val = macc['reduction'][i]['uom'] if i < len(macc['reduction']) and 'uom' in macc['reduction'][
                i] else 'kg'

            macc['reduction'][i]['material'] = cols[0].text_input(f"Material {i + 1}",
                                                                  value=material_val,
                                                                  key=f"red_mat_tab1_{i}")
            macc['reduction'][i]['quantity'] = cols[1].text_input(f"Quantity Reduced",
                                                                  value=quantity_val,
                                                                  key=f"red_qty_tab1_{i}")
            macc['reduction'][i]['uom'] = cols[2].selectbox(f"UOM {i + 1}", UOM_OPTIONS,
                                                            index=UOM_OPTIONS.index(
                                                                uom_val) if uom_val in UOM_OPTIONS else 0,
                                                            key=f"red_uom_tab1_{i}")

        st.markdown("**Materials/Energy Added (After Scenario)**")
        for i in range(3):
            cols = st.columns(3)
            # Safely get values with defaults
            material_val = macc['addition'][i]['material'] if i < len(macc['addition']) and 'material' in \
                                                              macc['addition'][i] else ''
            quantity_val = macc['addition'][i]['quantity'] if i < len(macc['addition']) and 'quantity' in \
                                                              macc['addition'][i] else ''
            uom_val = macc['addition'][i]['uom'] if i < len(macc['addition']) and 'uom' in macc['addition'][i] else 'kg'

            macc['addition'][i]['material'] = cols[0].text_input(f"Material {i + 1}",
                                                                 value=material_val,
                                                                 key=f"add_mat_tab1_{i}")
            macc['addition'][i]['quantity'] = cols[1].text_input(f"Quantity Added",
                                                                 value=quantity_val,
                                                                 key=f"add_qty_tab1_{i}")
            macc['addition'][i]['uom'] = cols[2].selectbox(f"UOM {i + 1}", UOM_OPTIONS,
                                                           index=UOM_OPTIONS.index(
                                                               uom_val) if uom_val in UOM_OPTIONS else 0,
                                                           key=f"add_uom_tab1_{i}")

    with tab2:
        st.subheader("Financial & Emission Parameters")

        # Create two columns for O1 and O2
        col_o1, col_o2 = st.columns(2)

        with col_o1:
            st.markdown(f"### {macc['option1'].get('label', 'Before Scenario')}")
            option_fields_tab2("o1_tab2", macc['option1'], is_o1=True)

        with col_o2:
            st.markdown(f"### {macc['option2'].get('label', 'After Scenario')}")
            option_fields_tab2("o2_tab2", macc['option2'], is_o1=False)

        # Calculate button between the columns
        st.markdown("---")
        if st.button("Calculate MACC", type="primary", key="calculate_macc_tab2", use_container_width=True):
            def calculate_npv_detailed(opt):
                """Calculate NPV with more detailed cashflows"""
                investment = opt['capex_own'] if opt['capex_type'] == "Own Investment" else opt['capex_loan_principal']

                # Calculate annual cashflows
                cashflows = [-investment]

                for year in range(1, opt['lifetime'] + 1):
                    # Annual benefit with decline
                    if year <= opt['benefit_duration']:
                        annual_benefit = opt['annual_benefit'] * ((1 - opt['benefit_decline_rate'] / 100) ** (year - 1))
                    else:
                        annual_benefit = 0

                    # Operating costs with inflation
                    opex_regular = opt['opex_regular_costs'] * ((1 + opt['inflation_rate'] / 100) ** (year - 1))
                    opex_fuel = opt['opex_fuel_energy_cost'] * ((1 + opt['fuel_energy_inflation'] / 100) ** (year - 1))

                    net_cashflow = annual_benefit - opex_regular - opex_fuel
                    cashflows.append(net_cashflow)

                # Add salvage value if applicable
                if opt['salvage_value'] > 0 and opt['year_of_salvage'] <= opt['lifetime']:
                    cashflows[opt['year_of_salvage']] += opt['salvage_value']

                # Add residual value at end
                cashflows[-1] += opt['residual_value']

                # Calculate NPV
                discount_rate = opt['discount_rate'] / 100
                npv = sum([cf / (1 + discount_rate) ** t for t, cf in enumerate(cashflows)])

                return npv

            npv1 = calculate_npv_detailed(macc['option1'])
            npv2 = calculate_npv_detailed(macc['option2'])
            diff_npv = npv1 - npv2

            annual_diff_co2 = macc['option1']['co2_reduction'] - macc['option2']['co2_reduction']
            tracking_period = max(macc['option1']['emission_tracking_period'],
                                  macc['option2']['emission_tracking_period'], 1)
            diff_co2 = annual_diff_co2 * tracking_period

            mac = diff_npv / diff_co2 if diff_co2 != 0 else 0

            result = f"""
{macc['option1'].get('label', 'Before Scenario')} NPV: ₹{npv1:,.2f}
{macc['option2'].get('label', 'After Scenario')} NPV: ₹{npv2:,.2f}
Net NPV ({macc['option1'].get('label', 'Before')} - {macc['option2'].get('label', 'After')}): ₹{diff_npv:,.2f}
Annual CO₂e Difference: {annual_diff_co2:,.0f} tons/year
Tracking Period: {tracking_period} years
Total CO₂e Difference: {diff_co2:,.0f} tons
MAC Value: ₹{mac:,.2f}/ton CO₂e
"""
            macc['result'] = result

            macc['calculated_npv1'] = npv1
            macc['calculated_npv2'] = npv2
            macc['calculated_mac'] = mac
            macc['total_co2_diff'] = diff_co2

            st.success("Calculation Complete!")
            st.rerun()

    with tab3:
        st.subheader("Calculation Results")

        # Display results if calculated
        if macc['result']:
            st.markdown("### MACC Calculation Results")
            st.code(macc['result'])

            # Display metrics
            col1, col2, col3, col4 = st.columns(4)
            col1.metric(f"{macc['option1'].get('label', 'Before')} NPV", f"₹{macc['calculated_npv1']:,.2f}")
            col2.metric(f"{macc['option2'].get('label', 'After')} NPV", f"₹{macc['calculated_npv2']:,.2f}")
            col3.metric("Net NPV Difference", f"₹{macc['calculated_npv1'] - macc['calculated_npv2']:,.2f}")
            col4.metric("MAC Value", f"₹{macc['calculated_mac']:,.2f}/ton")

            # Display CO2 metrics
            st.markdown("### CO₂ Emission Impact")
            col5, col6, col7 = st.columns(3)
            col5.metric("Annual CO₂ Reduction",
                        f"{abs(macc['option1']['co2_reduction'] - macc['option2']['co2_reduction']):,.0f} tons/year")
            col6.metric("Total CO₂ Reduction", f"{abs(macc['total_co2_diff']):,.0f} tons")
            col7.metric("Abatement Cost", f"₹{macc['calculated_mac']:,.2f}/ton")
        else:
            st.info("Click 'Calculate MACC' button in the Option Parameters tab to see results here.")

def option_fields_tab2(prefix, opt, is_o1=True):
    """Render option parameters with O1 as Before and O2 as After for Tab 2"""

    # Fixed labels - remove editable field
    if is_o1:
        scenario_label = "Before Scenario"
    else:
        scenario_label = "After Scenario"

    # Use fixed label (don't allow editing)
    opt['label'] = scenario_label
    st.markdown(f"### {scenario_label}")

    # CAPEX Section - Own Investment and Loan side by side
    st.markdown("#### CAPEX")
    capex_type = opt.get('capex_type', 'Own Investment')

    # Use horizontal radio button for side by side display
    opt['capex_type'] = st.radio("CAPEX Type", ["Own Investment", "Loan"],
                                 index=0 if capex_type == "Own Investment" else 1,
                                 key=f"{prefix}_capex_type",
                                 horizontal=True)

    # Show CAPEX fields based on selection
    if opt['capex_type'] == "Own Investment":
        opt['capex_own'] = st.number_input("CAPEX Amount (₹)",
                                           value=float(opt.get('capex_own', 0.0)),
                                           key=f"{prefix}_own")
    else:
        col_loan1, col_loan2, col_loan3 = st.columns(3)
        with col_loan1:
            opt['capex_loan_principal'] = st.number_input(
                "Principal Amount (₹)",
                value=float(opt.get('capex_loan_principal', 0.0)),
                key=f"{prefix}_loan_principal"
            )
        with col_loan2:
            opt['capex_loan_interest'] = st.number_input(
                "Interest Rate (%)",
                value=float(opt.get('capex_loan_interest', 0.0)),
                key=f"{prefix}_loan_interest"
            )
        with col_loan3:
            opt['capex_loan_period'] = st.number_input(
                "Repayment Period (Years)",
                value=int(opt.get('capex_loan_period', 0)),
                min_value=0,
                key=f"{prefix}_loan_period"
            )

    # Reinvestment Section
    st.markdown("#### Reinvestment")
    col_reinvest1, col_reinvest2, col_reinvest3 = st.columns(3)
    with col_reinvest1:
        opt['reinvestment'] = st.checkbox("Reinvestment Required",
                                          value=opt.get('reinvestment', False),
                                          key=f"{prefix}_reinvest_check")

    if opt['reinvestment']:
        with col_reinvest2:
            opt['reinvestment_year'] = st.number_input("Year of Reinvestment",
                                                       value=int(opt.get('reinvestment_year', 0)),
                                                       min_value=0,
                                                       key=f"{prefix}_reinvest_year")
        with col_reinvest3:
            opt['reinvestment_amount'] = st.number_input("Amount (₹)",
                                                         value=float(opt.get('reinvestment_amount', 0.0)),
                                                         key=f"{prefix}_reinvest_amt")

    # Operating Expenses & Inflation Rates Section - ALL IN SINGLE LINE
    st.markdown("#### Operating Expenses & Inflation Rates")

    # Create 4 columns for all OPEX and Inflation fields
    col_opex1, col_inf1, col_opex2, col_inf2 = st.columns(4)

    with col_opex1:
        st.markdown("**OPEX-Other than Fuel/Energy**")
        opt['opex_regular_costs'] = st.number_input("Amount (₹)",
                                                    value=float(opt.get('opex_regular_costs', 0.0)),
                                                    key=f"{prefix}_opex_reg")

    with col_inf1:
        st.markdown("**General Inflation Rate**")
        opt['inflation_rate'] = st.number_input("Rate (%)",
                                                value=float(opt.get('inflation_rate', 0.0)),
                                                key=f"{prefix}_inflation")

    with col_opex2:
        st.markdown("**OPEX-Only Fuel/Energy**")
        opt['opex_fuel_energy_cost'] = st.number_input("Amount (₹)",
                                                       value=float(opt.get('opex_fuel_energy_cost', 0.0)),
                                                       key=f"{prefix}_opex_fuel")

    with col_inf2:
        st.markdown("**Fuel/Energy Inflation Rate**")
        opt['fuel_energy_inflation'] = st.number_input("Rate (%)",
                                                       value=float(opt.get('fuel_energy_inflation', 0.0)),
                                                       key=f"{prefix}_fuel_inflation")

    # Asset Values (Removed Residual Value)
    st.markdown("#### Asset Values")
    col_asset1, col_asset2 = st.columns(2)
    with col_asset1:
        opt['salvage_value'] = st.number_input("Salvage Value (₹)",
                                               value=float(opt.get('salvage_value', 0.0)),
                                               key=f"{prefix}_salvage")
    with col_asset2:
        opt['year_of_salvage'] = st.number_input("Year of Salvage",
                                                 value=int(opt.get('year_of_salvage', 0)),
                                                 min_value=0,
                                                 key=f"{prefix}_salvage_year")

    # Benefits Section
    st.markdown("#### Benefits")
    col_ben1, col_ben2, col_ben3 = st.columns(3)
    with col_ben1:
        opt['annual_benefit'] = st.number_input("Annual Benefit (₹)",
                                                value=float(opt.get('annual_benefit', 0.0)),
                                                key=f"{prefix}_benefit")
    with col_ben2:
        opt['benefit_duration'] = st.number_input("Duration (Years)",
                                                  value=int(opt.get('benefit_duration', 0)),
                                                  min_value=0,
                                                  key=f"{prefix}_benefit_dur")
    with col_ben3:
        opt['benefit_decline_rate'] = st.number_input("Decline Rate (%)",
                                                      value=float(opt.get('benefit_decline_rate', 0.0)),
                                                      key=f"{prefix}_benefit_decline")

    # Project Parameters (Only 2 fields now)
    st.markdown("#### Project Parameters")
    col_param1, col_param2 = st.columns(2)
    with col_param1:
        opt['lifetime'] = st.number_input("Project Lifetime (Years)",
                                          value=int(opt.get('lifetime', 10)),
                                          min_value=1,
                                          key=f"{prefix}_lifetime")
    with col_param2:
        opt['discount_rate'] = st.number_input("Discount Rate (%)",
                                               value=float(opt.get('discount_rate', 8.0)),
                                               key=f"{prefix}_discount")

    # CO₂ Emissions & Tracking Section
    st.markdown("#### CO₂ Emissions & Tracking")

    col_co2, col_tracking = st.columns(2)

    with col_co2:
        if is_o1:
            co2_label = "CO₂e Reduction Target (tons)"
        else:
            co2_label = "CO₂e Reduction Achieved (tons)"

        opt['co2_reduction'] = st.number_input(co2_label,
                                               value=float(opt.get('co2_reduction', 0.0)),
                                               key=f"{prefix}_co2")

    with col_tracking:
        opt['emission_tracking_period'] = st.number_input("Emission Tracking (Years)",
                                                          value=int(opt.get('emission_tracking_period', 10)),
                                                          min_value=1,
                                                          key=f"{prefix}_tracking")


# --- Strategy Dashboard UI (completely redesigned with proper button handling) ---
def strategy_dashboard_ui():
    st.header("🌿 Decarbonization Strategy Dashboard")

    # Initialize session state for button actions
    if 'strategy_action' not in st.session_state:
        st.session_state.strategy_action = None

    # Initialize session state for confirmation
    if 'confirm_delete_id' not in st.session_state:
        st.session_state.confirm_delete_id = None

    # Initialize session state for loaded dashboard
    if 'loaded_dashboard_id' not in st.session_state:
        st.session_state.loaded_dashboard_id = None

    # Initialize session state for new dashboard name
    if 'new_dashboard_name' not in st.session_state:
        st.session_state.new_dashboard_name = "My Strategy Portfolio"

    # === Database Setup ===
    STRATEGY_DB = "strategy_dashboards.db"

    # Initialize strategy database
    conn = sqlite3.connect(STRATEGY_DB)
    conn.execute('''
        CREATE TABLE IF NOT EXISTS strategy_portfolios (
            id TEXT PRIMARY KEY,
            name TEXT UNIQUE,
            organization TEXT,
            sector TEXT,
            baseline_calc_id TEXT,
            selected_macc_projects TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    conn.commit()
    conn.close()

    # Function to refresh dashboard list
    def refresh_dashboard_list():
        """Refresh the list of saved dashboards from database"""
        try:
            conn = sqlite3.connect(STRATEGY_DB)
            saved_dashboards = pd.read_sql_query(
                "SELECT id, name, organization, sector, created_at FROM strategy_portfolios ORDER BY created_at DESC",
                conn
            )
            conn.close()
            return saved_dashboards
        except:
            return pd.DataFrame(columns=['id', 'name', 'organization', 'sector', 'created_at'])

    # Get current dashboard list
    saved_dashboards = refresh_dashboard_list()

    # Function to get unique dashboard name
    def get_unique_dashboard_name(base_name="My Strategy Portfolio"):
        """Generate a unique dashboard name"""
        if saved_dashboards.empty:
            return base_name

        existing_names = set(saved_dashboards['name'].tolist())
        if base_name not in existing_names:
            return base_name

        counter = 1
        while f"{base_name} ({counter})" in existing_names:
            counter += 1
        return f"{base_name} ({counter})"

    # === Main Layout ===
    # Create two main columns: left for management, right for display
    col_left, col_right = st.columns([1, 2])

    with col_left:
        st.subheader("📊 Dashboard Management")

        # Action Buttons Section
        st.markdown("### Actions")

        # Create 2 columns for buttons
        btn_col1, btn_col2 = st.columns(2)

        # New Button
        if btn_col1.button("🆕 New", use_container_width=True, key="btn_new_strategy"):
            st.session_state.strategy_action = "new"
            st.session_state.loaded_dashboard_id = None
            st.session_state.new_dashboard_name = get_unique_dashboard_name()
            st.rerun()

        # Load Button
        if btn_col2.button("📂 Load", use_container_width=True, key="btn_load_strategy"):
            st.session_state.strategy_action = "load"
            st.rerun()

        # If Load action is selected, show dashboard selection
        if st.session_state.strategy_action == "load":
            st.markdown("---")
            st.markdown("#### Select Dashboard to Load")

            if not saved_dashboards.empty:
                # Create a selection list
                dashboard_options = saved_dashboards['name'].tolist()
                selected_name = st.selectbox(
                    "Choose Dashboard",
                    options=["(Select a dashboard)"] + dashboard_options,
                    key="load_dashboard_select"
                )

                if selected_name != "(Select a dashboard)":
                    # Find the selected dashboard
                    selected_row = saved_dashboards[saved_dashboards['name'] == selected_name].iloc[0]
                    dashboard_id = selected_row['id']

                    col_load1, col_load2 = st.columns(2)
                    if col_load1.button("✅ Load Selected", use_container_width=True, key="btn_confirm_load"):
                        # Load the dashboard data
                        conn = sqlite3.connect(STRATEGY_DB)
                        row = conn.execute(
                            "SELECT name, organization, sector, baseline_calc_id, selected_macc_projects FROM strategy_portfolios WHERE id = ?",
                            (dashboard_id,)
                        ).fetchone()
                        conn.close()

                        if row:
                            loaded_name, loaded_org, loaded_sector, loaded_baseline_id, loaded_macc_str = row

                            # Store loaded data in session state
                            st.session_state.loaded_dashboard_id = dashboard_id
                            st.session_state.current_org_name = loaded_org
                            st.session_state.current_sector = loaded_sector
                            st.session_state.selected_calc_id = loaded_baseline_id

                            # Load MACC projects
                            if loaded_macc_str:
                                try:
                                    loaded_macc_ids = ast.literal_eval(loaded_macc_str)
                                    all_projects = get_saved_macc_projects()
                                    loaded_names = [p['name'] for p in all_projects if p['id'] in loaded_macc_ids]
                                    st.session_state.strategy_macc_select = loaded_names
                                except:
                                    st.session_state.strategy_macc_select = []

                            st.success(f"✅ Loaded: **{loaded_name}**")
                            st.session_state.strategy_action = None
                            st.rerun()

                    if col_load2.button("❌ Cancel", use_container_width=True, key="btn_cancel_load"):
                        st.session_state.strategy_action = None
                        st.rerun()
            else:
                st.info("No saved dashboards found.")

        # Save/Update Button
        if st.session_state.loaded_dashboard_id:
            # Update button for existing dashboard
            if btn_col1.button("🔄 Update", use_container_width=True, key="btn_update_strategy"):
                st.session_state.strategy_action = "update"
                st.rerun()
        else:
            # Save button for new dashboard
            if btn_col2.button("💾 Save", use_container_width=True, key="btn_save_strategy"):
                st.session_state.strategy_action = "save"
                st.rerun()

        # Delete Button (only shown if a dashboard is loaded)
        if st.session_state.loaded_dashboard_id and st.session_state.loaded_dashboard_id in saved_dashboards[
            'id'].values:
            if btn_col2.button("🗑️ Delete", use_container_width=True, key="btn_delete_strategy", type="secondary"):
                st.session_state.confirm_delete_id = st.session_state.loaded_dashboard_id
                st.rerun()

        # Handle Save/Update action
        if st.session_state.strategy_action in ["save", "update"]:
            st.markdown("---")
            if st.session_state.strategy_action == "save":
                st.markdown("#### Save New Dashboard")
                default_name = get_unique_dashboard_name()
                if 'new_dashboard_name' in st.session_state:
                    default_name = st.session_state.new_dashboard_name
            else:
                st.markdown("#### Update Dashboard")
                # Get current dashboard name
                current_name = ""
                if st.session_state.loaded_dashboard_id:
                    conn = sqlite3.connect(STRATEGY_DB)
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM strategy_portfolios WHERE id = ?",
                                   (st.session_state.loaded_dashboard_id,))
                    result = cursor.fetchone()
                    if result:
                        current_name = result[0]
                    conn.close()
                default_name = current_name

            # Dashboard name input
            dashboard_name = st.text_input(
                "Dashboard Name",
                value=default_name,
                key="dashboard_name_input"
            )

            # Save/Update buttons
            col_save1, col_save2 = st.columns(2)

            if col_save1.button("✅ Confirm", use_container_width=True, key="btn_confirm_save"):
                if not dashboard_name.strip():
                    st.error("Please enter a dashboard name")
                else:
                    # Check for duplicate name (only for new saves, not for updates of same dashboard)
                    conn = sqlite3.connect(STRATEGY_DB)
                    cursor = conn.cursor()

                    if st.session_state.strategy_action == "save":
                        # Check if name exists for new save
                        cursor.execute("SELECT id FROM strategy_portfolios WHERE name = ?", (dashboard_name.strip(),))
                        existing = cursor.fetchone()
                        if existing:
                            st.error(f"❌ Dashboard name '{dashboard_name.strip()}' already exists.")
                            conn.close()
                            return

                    # Get current data from session state
                    current_org = st.session_state.get('current_org_name', 'Unknown')
                    current_sector = st.session_state.get('current_sector', 'Unknown')
                    current_calc_id = st.session_state.get('selected_calc_id', '')
                    current_macc = st.session_state.get('strategy_macc_select', [])
                    all_projects = get_saved_macc_projects()
                    macc_ids = [p['id'] for p in all_projects if p['name'] in current_macc]

                    # Determine ID
                    if st.session_state.strategy_action == "save" or not st.session_state.loaded_dashboard_id:
                        save_id = str(uuid.uuid4())[:8]
                    else:
                        save_id = st.session_state.loaded_dashboard_id

                    # Save to database
                    cursor.execute('''
                        INSERT OR REPLACE INTO strategy_portfolios 
                        (id, name, organization, sector, baseline_calc_id, selected_macc_projects, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (
                        save_id,
                        dashboard_name.strip(),
                        current_org,
                        current_sector,
                        current_calc_id,
                        str(macc_ids)
                    ))
                    conn.commit()
                    conn.close()

                    st.success(
                        f"✅ Dashboard {'saved' if st.session_state.strategy_action == 'save' else 'updated'}: **{dashboard_name.strip()}**")

                    # Update session state
                    st.session_state.loaded_dashboard_id = save_id
                    st.session_state.strategy_action = None
                    st.rerun()

            if col_save2.button("❌ Cancel", use_container_width=True, key="btn_cancel_save"):
                st.session_state.strategy_action = None
                st.rerun()

        # Handle Delete confirmation
        if st.session_state.confirm_delete_id:
            st.markdown("---")
            st.markdown("#### Confirm Deletion")
            st.warning("⚠️ Are you sure you want to delete this dashboard? This action cannot be undone.")

            # Get dashboard name
            dashboard_name = ""
            conn = sqlite3.connect(STRATEGY_DB)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM strategy_portfolios WHERE id = ?", (st.session_state.confirm_delete_id,))
            result = cursor.fetchone()
            if result:
                dashboard_name = result[0]
            conn.close()

            col_del1, col_del2 = st.columns(2)

            if col_del1.button("✅ Yes, Delete", use_container_width=True, type="primary", key="btn_confirm_delete"):
                conn = sqlite3.connect(STRATEGY_DB)
                conn.execute("DELETE FROM strategy_portfolios WHERE id = ?", (st.session_state.confirm_delete_id,))
                conn.commit()
                conn.close()

                # Clear session state if deleting loaded dashboard
                if st.session_state.loaded_dashboard_id == st.session_state.confirm_delete_id:
                    st.session_state.loaded_dashboard_id = None
                    keys_to_clear = ['current_org_name', 'current_sector', 'selected_calc_id', 'strategy_macc_select']
                    for key in keys_to_clear:
                        if key in st.session_state:
                            del st.session_state[key]

                st.success(f"✅ Dashboard '{dashboard_name}' deleted successfully!")
                st.session_state.confirm_delete_id = None
                st.rerun()

            if col_del2.button("❌ Cancel", use_container_width=True, key="btn_cancel_delete"):
                st.session_state.confirm_delete_id = None
                st.rerun()

        # Generate Strategy Report Button
        st.markdown("---")
        if st.button("📈 Generate Strategy Report", use_container_width=True, key="btn_generate_report"):
            st.session_state.strategy_action = "generate"
            st.rerun()

        # Show current status
        st.markdown("---")
        st.markdown("### Current Status")
        if st.session_state.loaded_dashboard_id:
            # Get dashboard info
            conn = sqlite3.connect(STRATEGY_DB)
            cursor = conn.cursor()
            cursor.execute("SELECT name, organization, sector FROM strategy_portfolios WHERE id = ?",
                           (st.session_state.loaded_dashboard_id,))
            result = cursor.fetchone()
            conn.close()

            if result:
                st.info(f"**Loaded:** {result[0]}")
                st.caption(f"Organization: {result[1]}")
                st.caption(f"Sector: {result[2]}")
        else:
            st.info("No dashboard loaded. Create or load a dashboard to begin.")

    with col_right:
        # Display loaded dashboard or create new
        if st.session_state.loaded_dashboard_id:
            st.success(f"✅ Dashboard Loaded: Editing mode")
        else:
            st.info("ℹ️ Create a new dashboard or load an existing one to begin.")

        # === Dashboard Content ===

        # === 1. Select Organization from Fuel & Energy Calculations ===
        st.subheader("1. Select Organization")

        saved_calcs = []
        try:
            conn = sqlite3.connect(FUEL_DB_PATH)
            cursor = conn.cursor()
            cursor.execute("SELECT unique_code, org_name, sector FROM calculations ORDER BY created_at DESC")
            saved_calcs = cursor.fetchall()
            conn.close()
        except Exception as e:
            st.error(f"Error loading organizations: {e}")

        if not saved_calcs:
            st.info("No Fuel & Energy calculations found. Please create one first.")
            return

        calc_options = [f"{row[1] or 'Unnamed'} ({row[2] or 'N/A'}) - {row[0]}" for row in saved_calcs]
        calc_ids = [row[0] for row in saved_calcs]

        # Determine default index based on loaded data
        default_index = 0
        if st.session_state.loaded_dashboard_id and 'selected_calc_id' in st.session_state:
            loaded_calc_id = st.session_state.get('selected_calc_id', '')
            if loaded_calc_id:
                for i, calc_id in enumerate(calc_ids):
                    if calc_id == loaded_calc_id:
                        default_index = i
                        break

        # Create selection without storing in problematic session state key
        selected_label = st.selectbox(
            "Choose Organization & Baseline Calculation",
            options=calc_options,
            index=default_index,
            key="strategy_org_select_widget"  # Different key to avoid conflicts
        )

        # Safely get the selected calculation
        try:
            if selected_label in calc_options:
                selected_index = calc_options.index(selected_label)
                selected_calc_id = calc_ids[selected_index]
            else:
                selected_calc_id = calc_ids[0] if calc_ids else ""
        except Exception as e:
            st.error(f"Error selecting calculation: {e}")
            selected_calc_id = calc_ids[0] if calc_ids else ""

        # Store in session state (not in widget key)
        st.session_state.selected_calc_id = selected_calc_id

        # Load calculation data
        if selected_calc_id:
            loaded = load_calculation_from_db(selected_calc_id)
            if not loaded:
                st.error("Failed to load selected calculation. The calculation may have been deleted.")
                return

            # Extract values
            current_org_name = loaded['meta'].get('org_name', 'Unknown Organization') or "Unknown Organization"
            current_sector = loaded['meta'].get('sector', 'Unknown Sector') or "Unknown Sector"
            baseline_year = loaded['meta'].get('baseline_year', 'N/A')
            previous_year = loaded['meta'].get('previous_year', baseline_year)
            target_year = loaded['meta'].get('target_year', baseline_year + 8)

            # Store in session state for saving
            st.session_state.current_org_name = current_org_name
            st.session_state.current_sector = current_sector

            # Display organization info
            col1, col2, col3 = st.columns(3)
            col1.metric("Organization", current_org_name)
            col2.metric("Sector", current_sector)
            col3.metric("Baseline Year", baseline_year)

            # Get calculation data from session state
            calc_data = {}
            if 'calc' in st.session_state:
                calc_data = st.session_state.calc

            # Get same_year flag
            same_year = calc_data.get('same_year', False) if calc_data else False

            # Emission values
            baseline_input = loaded.get('baseline_input', {"1": 0.0, "2": 0.0, "3": 0.0})
            baseline_emission = sum(baseline_input.values())

            df_rows = pd.DataFrame(loaded['baseline_rows'])
            previous_emission = df_rows['emission'].sum() if not df_rows.empty else 0.0

            # Calculate target emissions based on reductions
            reductions_pct = loaded.get('reductions_pct', {"Scope 1": 0.0, "Scope 2": 0.0, "Scope 3": 0.0})
            total_planned_reduction = sum(
                baseline_emission * (reductions_pct[scope] / 100)
                for scope in ["Scope 1", "Scope 2", "Scope 3"]
            )

            # Calculate BAU emissions
            # Get production data
            baseline_production = loaded['meta'].get('baseline_production', 1.0)
            previous_year_production = loaded['meta'].get('previous_year_production', baseline_production)
            growth_rate = loaded['meta'].get('growth_rate', 0.05)
            target_production = loaded['meta'].get('target_production',
                                                   baseline_production * (
                                                               (1 + growth_rate) ** (target_year - baseline_year)))

            # BAU = Previous emissions adjusted for production growth
            production_growth_factor = target_production / previous_year_production if previous_year_production != 0 else 1.0
            total_bau = previous_emission * production_growth_factor

            # === 2. MACC Projects Selection ===
            st.subheader("2. Select MACC Projects")

            projects = get_saved_macc_projects()
            if not projects:
                st.info("No MACC projects found. Please create and save projects in the **MACC Calculator**.")
                total_reduction_macc = 0
                portfolio = pd.DataFrame()
            else:
                df_projects = pd.DataFrame(projects)

                # Get default selection from session state
                default_macc = st.session_state.get('strategy_macc_select', df_projects['name'].tolist())

                selected_projects = st.multiselect(
                    "Select projects to include in strategy",
                    options=df_projects['name'].tolist(),
                    default=default_macc,
                    key="strategy_macc_select_widget"  # Different key
                )

                # Store in session state
                st.session_state.strategy_macc_select = selected_projects

                portfolio = df_projects[df_projects['name'].isin(selected_projects)]
                total_reduction_macc = portfolio['co2_reduction'].sum()

                # Display project summary
                if not portfolio.empty:
                    st.markdown(f"**Selected Projects:** {len(portfolio)}")
                    st.markdown(f"**Total Annual CO₂ Reduction:** {total_reduction_macc:,.0f} tCO₂e")

                    # Show quick project list
                    with st.expander("View Selected Projects"):
                        for idx, row in portfolio.iterrows():
                            st.write(
                                f"• **{row['name']}**: {row['co2_reduction']:,.0f} tCO₂e/year (MAC: ₹{row['mac']:,.0f}/ton)")

                # === 3. Strategy Analysis ===
                st.subheader("3. Strategy Analysis")

                # Key Metrics
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Baseline Emissions", f"{baseline_emission:,.0f} tCO₂e")
                col2.metric("Previous Year", f"{previous_emission:,.0f} tCO₂e")
                col3.metric("Planned Reduction", f"{total_planned_reduction:,.0f} tCO₂e")
                col4.metric("MACC Achieved", f"{total_reduction_macc:,.0f} tCO₂e")

                remaining_after_macc = baseline_emission - total_reduction_macc
                achieved_percentage = (total_reduction_macc / baseline_emission * 100) if baseline_emission > 0 else 0

                col5, col6 = st.columns(2)
                col5.metric("Remaining After MACC", f"{remaining_after_macc:,.0f} tCO₂e")
                col6.metric("Abatement Achieved", f"{achieved_percentage:.1f}%")

                # Waterfall Chart - Fixed to show all requested steps
                if not portfolio.empty:
                    st.subheader("Annual CO₂ Emission Pathway")

                    # 1. Baseline Emission
                    baseline_step = baseline_emission

                    # 2. Drop from Baseline to Previous Year (negative)
                    drop_to_previous = previous_emission - baseline_emission  # This will be negative

                    # 3. Previous Year Total Emission
                    previous_year_total = previous_emission

                    # 4. Total Increase from Previous Year to Target BAU
                    # BAU is Previous * Growth Factor, so increase = BAU - Previous
                    bau_increase = total_bau - previous_emission  # This will be positive

                    # 5. Total BAU Emission
                    bau_total = total_bau

                    # 6. Selected Projects (reductions - negative values)
                    project_names = [f"{row['name']}<br>(Reduction)" for _, row in portfolio.iterrows()]
                    project_reductions = [-row['co2_reduction'] for _, row in portfolio.iterrows()]

                    # 7. Final Remaining CO₂e (Annual) = BAU - Total Reductions
                    final_remaining = bau_total - total_reduction_macc

                    # Create the waterfall steps
                    steps = [
                        f"Baseline<br>(Year {baseline_year})",
                        f"Drop to {previous_year}",
                        f"Previous Year<br>({previous_year})",
                        f"Growth to BAU",
                        f"Business-As-Usual<br>({target_year})"
                    ]

                    values = [
                        baseline_step,
                        drop_to_previous,  # Negative (decrease)
                        previous_year_total,
                        bau_increase,  # Positive (increase)
                        bau_total
                    ]

                    measures = [
                        "absolute",  # Baseline is absolute starting point
                        "relative",  # Drop is relative change
                        "total",  # Previous Year is total so far
                        "relative",  # Growth is relative change
                        "total"  # BAU is total so far
                    ]

                    # Add project reduction steps
                    steps.extend(project_names)
                    values.extend(project_reductions)
                    measures.extend(["relative"] * len(project_names))

                    # Add final remaining step
                    steps.append(f"Final Remaining<br>(After MACC)")
                    values.append(final_remaining)
                    measures.append("total")

                    # Create text labels
                    text_labels = []
                    for i, (step, val) in enumerate(zip(steps, values)):
                        if i == 0:  # Baseline
                            text_labels.append(f"{val:,.0f}")
                        elif i == 1:  # Drop to previous year (will be negative)
                            text_labels.append(f"{val:+,.0f}")
                        elif i == 2:  # Previous year total
                            text_labels.append(f"{val:,.0f}")
                        elif i == 3:  # Growth to BAU (will be positive)
                            text_labels.append(f"{val:+,.0f}")
                        elif i == 4:  # BAU total
                            text_labels.append(f"{val:,.0f}")
                        elif i < len(steps) - 1:  # Project reductions
                            # Project reductions are already negative in values
                            text_labels.append(f"{val:+,.0f}")
                        else:  # Final remaining
                            text_labels.append(f"{val:,.0f}")

                    # Create the waterfall chart
                    fig = go.Figure(go.Waterfall(
                        name="Annual CO₂ Flow",
                        orientation="v",
                        measure=measures,
                        x=steps,
                        y=values,
                        text=text_labels,
                        textposition="outside",
                        connector={"line": {"color": "rgb(63, 63, 63)", "width": 1}},
                        increasing={"marker": {"color": "#FF6B6B"}},  # Red for increases
                        decreasing={"marker": {"color": "#4ECDC4"}},  # Green for decreases
                        totals={"marker": {"color": "#45B7D1"}}  # Blue for totals
                    ))

                    # Add annotations for key points
                    annotations = []

                    # Add an annotation for Baseline
                    annotations.append(dict(
                        x=0,
                        y=baseline_step,
                        text=f"Baseline: {baseline_step:,.0f} tCO₂e",
                        showarrow=True,
                        arrowhead=2,
                        ax=0,
                        ay=-40,
                        font=dict(size=10)
                    ))

                    # Add annotation for BAU
                    annotations.append(dict(
                        x=4,  # BAU is at index 4
                        y=bau_total,
                        text=f"BAU: {bau_total:,.0f} tCO₂e",
                        showarrow=True,
                        arrowhead=2,
                        ax=0,
                        ay=40,
                        font=dict(size=10)
                    ))

                    # Add annotation for Final Remaining
                    annotations.append(dict(
                        x=len(steps) - 1,
                        y=final_remaining,
                        text=f"Final: {final_remaining:,.0f} tCO₂e",
                        showarrow=True,
                        arrowhead=2,
                        ax=0,
                        ay=-40,
                        font=dict(size=10)
                    ))

                    # Calculate total reduction achieved
                    total_avoided = total_bau - final_remaining
                    if total_avoided > 0:
                        annotations.append(dict(
                            x=len(steps) - 2,  # Position near final
                            y=final_remaining + total_avoided / 2,
                            text=f"Total Avoided:<br>{total_avoided:,.0f} tCO₂e",
                            showarrow=False,
                            font=dict(size=10, color="green"),
                            align="center",
                            bgcolor="rgba(255,255,255,0.8)"
                        ))

                    fig.update_layout(
                        title=f"Annual CO₂ Emission Pathway – {current_org_name}",
                        yaxis_title="tCO₂e / Year",
                        xaxis_tickangle=-45,
                        template="plotly_white",
                        showlegend=False,
                        height=600,
                        margin=dict(t=80, b=150, l=60, r=20),  # Extra bottom margin for rotated labels
                        annotations=annotations
                    )

                    # Add summary information
                    col1, col2, col3 = st.columns(3)
                    col1.metric("Baseline Emissions", f"{baseline_emission:,.0f} tCO₂e")
                    col2.metric("BAU Emissions", f"{total_bau:,.0f} tCO₂e",
                                delta=f"{bau_increase:+,.0f} tCO₂e" if bau_increase != 0 else "No change")
                    col3.metric("Final Remaining", f"{final_remaining:,.0f} tCO₂e",
                                delta=f"-{total_reduction_macc:,.0f} tCO₂e" if total_reduction_macc > 0 else "No reduction")

                    st.plotly_chart(fig, use_container_width=True)

                    # Add explanatory text
                    with st.expander("📊 Waterfall Chart Explanation"):
                        st.markdown(f"""
                        **How to read this chart:**

                        1. **Baseline ({baseline_year})**: Starting point at {baseline_emission:,.0f} tCO₂e
                        2. **Drop to {previous_year}**: Change of {drop_to_previous:+,.0f} tCO₂e from baseline to previous year
                        3. **Previous Year ({previous_year})**: Total emissions of {previous_emission:,.0f} tCO₂e
                        4. **Growth to BAU**: Increase of {bau_increase:+,.0f} tCO₂e due to production growth
                        5. **Business-As-Usual ({target_year})**: Projected emissions without MACC projects: {total_bau:,.0f} tCO₂e
                        6. **MACC Projects**: Reductions from {len(portfolio)} selected projects
                        7. **Final Remaining**: Emissions after implementing all MACC projects: {final_remaining:,.0f} tCO₂e

                        **Key Metrics:**
                        - **Total Avoided Emissions**: {total_reduction_macc:,.0f} tCO₂e
                        - **BAU vs Baseline Increase**: {bau_increase:+,.0f} tCO₂e ({bau_increase / baseline_emission * 100:.1f}%)
                        - **Final vs Baseline Reduction**: {baseline_emission - final_remaining:+,.0f} tCO₂e ({(1 - final_remaining / baseline_emission) * 100:.1f}%)
                        """)

                    # MACC Chart - Fixed to show bars side by side without gaps
                    st.subheader("Marginal Abatement Cost Curve")

                    macc_portfolio = portfolio.copy()
                    # Sort by MAC value (from negative to positive)
                    macc_portfolio = macc_portfolio.sort_values('mac').reset_index(drop=True)

                    # Calculate cumulative abatement for x-positioning
                    macc_portfolio['cumulative_co2'] = macc_portfolio['co2_reduction'].cumsum()
                    macc_portfolio['x_start'] = macc_portfolio['cumulative_co2'].shift(1, fill_value=0)
                    macc_portfolio['x_end'] = macc_portfolio['cumulative_co2']

                    # Create the MACC chart with proper bar positioning
                    fig_macc = go.Figure()

                    # Add bars for each project
                    for idx, row in macc_portfolio.iterrows():
                        # Determine bar color based on MAC value
                        if row['mac'] < 0:
                            color = '#4ECDC4'  # Green for cost-saving
                        elif row['mac'] == 0:
                            color = '#FFD166'  # Yellow for neutral
                        else:
                            color = '#FF6B6B'  # Red for cost-incurring

                        # Create custom hover text
                        hover_text = (
                            f"<b>{row['name']}</b><br>"
                            f"MAC: ₹{row['mac']:,.0f}/ton<br>"
                            f"Abated: {row['co2_reduction']:,.0f} tons<br>"
                            f"Cumulative: {row['cumulative_co2']:,.0f} tons"
                        )

                        # Add bar with custom width
                        fig_macc.add_trace(go.Bar(
                            x=[(row['x_start'] + row['x_end']) / 2],  # Center of bar
                            y=[row['mac']],
                            width=[row['co2_reduction']],  # Width based on abatement amount
                            name=row['name'],
                            text=[f"{row['name']}<br>₹{row['mac']:,.0f}/ton"],
                            textposition='outside',
                            marker_color=color,
                            hovertemplate=hover_text + "<extra></extra>",
                            showlegend=True
                        ))

                    # Update layout for side-by-side bars without gaps
                    fig_macc.update_layout(
                        title="Marginal Abatement Cost Curve",
                        xaxis_title="Cumulative CO₂e Abatement (tons)",
                        yaxis_title="MAC (₹ per ton CO₂e)",
                        template="plotly_white",
                        barmode='overlay',  # Changed from 'stack' to 'overlay' for side-by-side
                        bargap=0,  # No gap between bars
                        bargroupgap=0,  # No gap between bar groups
                        showlegend=True,
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="right",
                            x=1
                        ),
                        height=500,
                        xaxis=dict(
                            tickmode='array',
                            tickvals=macc_portfolio['cumulative_co2'].tolist(),
                            ticktext=[f"{val:,.0f}" for val in macc_portfolio['cumulative_co2']]
                        )
                    )

                    # Add horizontal line at MAC = 0 for reference
                    fig_macc.add_hline(
                        y=0,
                        line_dash="dash",
                        line_color="gray",
                        annotation_text="Zero Cost Line",
                        annotation_position="bottom right"
                    )

                    # Add grid and styling
                    fig_macc.update_xaxes(
                        showgrid=True,
                        gridwidth=1,
                        gridcolor='rgba(128, 128, 128, 0.2)',
                        zeroline=True,
                        zerolinecolor='gray',
                        zerolinewidth=1
                    )

                    fig_macc.update_yaxes(
                        showgrid=True,
                        gridwidth=1,
                        gridcolor='rgba(128, 128, 128, 0.2)',
                        zeroline=True,
                        zerolinecolor='gray',
                        zerolinewidth=1
                    )

                    st.plotly_chart(fig_macc, use_container_width=True)

                    # Add MACC statistics
                    with st.expander("📈 MACC Statistics", expanded=False):
                        col1, col2, col3, col4 = st.columns(4)

                        # Calculate statistics
                        total_abatement = macc_portfolio['co2_reduction'].sum()
                        avg_mac = macc_portfolio['mac'].mean()
                        negative_cost_projects = macc_portfolio[macc_portfolio['mac'] < 0]
                        positive_cost_projects = macc_portfolio[macc_portfolio['mac'] > 0]

                        col1.metric("Total Abatement", f"{total_abatement:,.0f} tCO₂e")
                        col2.metric("Average MAC", f"₹{avg_mac:,.0f}/ton")
                        col3.metric("Cost-Saving Projects", len(negative_cost_projects))
                        col4.metric("Cost-Incurring Projects", len(positive_cost_projects))

                        # Show cost breakdown
                        st.markdown("**Cost Breakdown:**")
                        total_savings = abs(
                            negative_cost_projects['cost'].sum()) if not negative_cost_projects.empty else 0
                        total_costs = positive_cost_projects['cost'].sum() if not positive_cost_projects.empty else 0
                        net_cost = total_costs - total_savings

                        st.write(f"- **Total Cost Savings:** ₹{total_savings:,.0f}")
                        st.write(f"- **Total Implementation Costs:** ₹{total_costs:,.0f}")
                        st.write(f"- **Net Cost:** ₹{net_cost:+,.0f}")

                        if net_cost < 0:
                            st.success(f"💰 **Net Savings:** ₹{abs(net_cost):,.0f}")
                        elif net_cost > 0:
                            st.warning(f"💸 **Net Cost:** ₹{net_cost:,.0f}")
                        else:
                            st.info("⚖️ **Cost Neutral**")
                    # Recommendations
                    st.subheader("Strategic Recommendations")

                    negative_mac = macc_portfolio[macc_portfolio['mac'] < 0]
                    if not negative_mac.empty:
                        st.success(f"**Cost-Saving Opportunities ({len(negative_mac)} projects):**")
                        for _, row in negative_mac.iterrows():
                            saving = abs(row['cost'])
                            st.write(
                                f"• **{row['name']}**: Abates {row['co2_reduction']:,.0f} tons, saves ₹{saving:,.0f}")

                    if achieved_percentage >= 50:
                        st.balloons()
                        st.success("🎉 Outstanding! You've achieved ≥50% abatement of baseline emissions.")
                    elif achieved_percentage >= 30:
                        st.success("👍 Strong progress toward decarbonization targets.")
                    else:
                        st.warning("Consider adding more projects to meet your reduction goals.")

        # Generate Strategy Report Section
        if st.session_state.strategy_action == "generate":
            st.markdown("---")
            st.subheader("📊 Strategy Report")

            if not saved_calcs:
                st.info("Please select an organization and projects first.")
            else:
                # Create report
                report_data = {
                    "Organization": st.session_state.get('current_org_name', 'Not selected'),
                    "Sector": st.session_state.get('current_sector', 'Not selected'),
                    "Baseline Emissions (tCO₂e)": f"{baseline_emission:,.0f}" if 'baseline_emission' in locals() else "N/A",
                    "Selected MACC Projects": len(portfolio) if 'portfolio' in locals() else 0,
                    "Total Annual Reduction (tCO₂e)": f"{total_reduction_macc:,.0f}" if 'total_reduction_macc' in locals() else "N/A",
                    "Abatement Achieved (%)": f"{achieved_percentage:.1f}%" if 'achieved_percentage' in locals() else "N/A",
                    "Report Generated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }

                # Display report
                for key, value in report_data.items():
                    st.write(f"**{key}:** {value}")

                # Export options
                st.markdown("---")
                st.markdown("#### Export Options")

                col_exp1, col_exp2 = st.columns(2)

                # CSV Export
                report_df = pd.DataFrame([report_data])
                csv = report_df.to_csv(index=False)
                col_exp1.download_button(
                    label="📥 Download as CSV",
                    data=csv,
                    file_name=f"strategy_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv",
                    key="download_csv_report"
                )

                # PDF Export (placeholder)
                if col_exp2.button("📄 Generate PDF Report", key="btn_pdf_report"):
                    st.info("PDF generation feature coming soon!")

                # Close report
                if st.button("Close Report", key="btn_close_report"):
                    st.session_state.strategy_action = None
                    st.rerun()


# Helper functions (outside the UI function)
def _safe_float(s):
    try:
        return float(str(s).strip()) if str(s).strip() != "" else 0.0
    except:
        return 0.0
def co2_project_calculator_ui():
    st.header("📊 Project CO₂ Emission Calculator with Actuals Tracking")

    CO2_DB_PATH = "co2_calculator.db"
    FUEL_DB_PATH = "fuel_energy.db"

    # Initialize session state
    if 'co2_project' not in st.session_state:
        st.session_state.co2_project = {
            'project_code': '',
            'organization': '',
            'entity_name': '',
            'unit_name': '',
            'project_name': '',
            'base_year': '',
            'target_year': '',
            'implementation_date': '',
            'life_span': '10',
            'project_owner': '',
            'input_data': [{}],
            'output_data': [{}],
            'costing_data': [{}],
            'amp_before': 0.0,
            'amp_after': 0.0,
            'amp_uom': 't/tp',
            'calculation_method': 'absolute',
            'is_loaded_from_db': False,
            'primary_output_before': 0.0,
            'primary_output_after': 0.0,
        }

    project = st.session_state.co2_project

    # --- Helper Functions ---
    def parse_json_safely(json_str, default_value):
        """Safely parse JSON with error handling"""
        if not json_str or json_str == 'null' or json_str == 'None':
            return default_value
        try:
            if isinstance(json_str, str):
                return json.loads(json_str)
            else:
                return json_str
        except (json.JSONDecodeError, TypeError):
            return default_value

    def normalize_data_row(row):
        """Normalize data row to have consistent field names"""
        if not isinstance(row, dict):
            return {}

        normalized = {}

        field_mapping = {
            'material': ['material', 'Material', 'name', 'Name'],
            'uom': ['uom', 'UOM', 'unit', 'Unit'],
            'ef': ['ef', 'Emission Factor (tCO₂e/unit)', 'emission_factor', 'emission'],
            'abs_before': ['abs_before', 'Absolute Before', 'abs_before_value', 'before_abs', 'Abs Actual-Before'],
            'abs_after': ['abs_after', 'Absolute After', 'abs_after_value', 'after_abs', 'Abs Planned-After'],
            'spec_before': ['spec_before', 'Specific Before', 'spec_before_value', 'before_spec', 'Spec Actual-Before'],
            'spec_after': ['spec_after', 'Specific After', 'spec_after_value', 'after_spec', 'Spec Planned-After']
        }

        for standard_field, possible_names in field_mapping.items():
            for name in possible_names:
                if name in row and row[name] is not None:
                    try:
                        if standard_field in ['ef', 'abs_before', 'abs_after', 'spec_before', 'spec_after']:
                            if isinstance(row[name], (int, float)):
                                normalized[standard_field] = float(row[name])
                            elif isinstance(row[name], str):
                                try:
                                    normalized[standard_field] = float(row[name])
                                except ValueError:
                                    normalized[standard_field] = 0.0
                            else:
                                normalized[standard_field] = 0.0
                        else:
                            normalized[standard_field] = str(row[name])
                        break
                    except (ValueError, TypeError):
                        normalized[standard_field] = 0.0 if standard_field in ['ef', 'abs_before', 'abs_after',
                                                                               'spec_before', 'spec_after'] else ''
        return normalized

    def enforce_specific_rules(data, calculation_method, data_type='output'):
        """Enforce rules for specific calculations"""
        if calculation_method == 'specific' and data_type == 'output':
            for row in data:
                row['abs_before'] = 1.0
                row['spec_before'] = 1.0
                row['spec_after'] = 1.0
        return data

    def calculate_tracking_emissions(input_values, output_values, amp_value, calculation_method, input_ef_list,
                                     output_ef_list, is_output_specific=False):
        """Calculate emissions for tracking based on actual values"""
        total_input_emission = 0.0
        total_output_emission = 0.0

        if calculation_method == 'absolute':
            for i, (input_val, input_ef) in enumerate(zip(input_values, input_ef_list)):
                total_input_emission += input_val * input_ef

            for i, (output_val, output_ef) in enumerate(zip(output_values, output_ef_list)):
                total_output_emission += output_val * output_ef

        else:  # specific
            for i, (input_val, input_ef) in enumerate(zip(input_values, input_ef_list)):
                total_input_emission += (amp_value * input_val) * input_ef

            for i, (output_val, output_ef) in enumerate(zip(output_values, output_ef_list)):
                if is_output_specific:
                    output_val = 1.0
                total_output_emission += (amp_value * output_val) * output_ef

        net_emission = total_input_emission - total_output_emission
        return total_input_emission, total_output_emission, net_emission

    def generate_project_id(organization, entity_name, unit_name, project_name, target_year, project_owner):
        """Generate a meaningful Project ID based on project details"""
        # Use first 2-3 letters of each component
        org_code = organization[:3].upper() if organization else "ORG"
        entity_code = entity_name[:2].upper() if entity_name else "EN"
        unit_code = unit_name[:2].upper() if unit_name else "UN"
        project_code = project_name[:3].upper() if project_name else "PRJ"
        owner_code = project_owner[:2].upper() if project_owner else "PO"

        # Generate ID: ORG-EN-UN-PRJ-2025-PO-XXXX
        base_id = f"{org_code}-{entity_code}-{unit_code}-{project_code}-{target_year}-{owner_code}"

        # Add unique suffix
        unique_suffix = uuid.uuid4().hex[:4].upper()

        return f"{base_id}-{unique_suffix}"

    # --- Get Organizations from Fuel & Energy Calculator ---
    def get_organizations_from_fuel_db():
        """Fetch organizations from Fuel & Energy Calculator database"""
        try:
            conn = sqlite3.connect(FUEL_DB_PATH)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT DISTINCT org_name FROM calculations WHERE org_name IS NOT NULL AND org_name != '' ORDER BY org_name")
            organizations = [row[0] for row in cursor.fetchall()]
            conn.close()
            return organizations
        except Exception as e:
            st.warning(f"Could not load organizations: {e}")
            return []

    # --- Load Projects for Dropdown ---
    def load_projects_for_dropdown():
        """Load projects from database and format for dropdown"""
        try:
            conn = sqlite3.connect(CO2_DB_PATH)
            projects_df = pd.read_sql_query(
                "SELECT project_code, organization, entity_name, unit_name, project_name, base_year, target_year, project_owner, calculation_method FROM projects ORDER BY created_at DESC",
                conn
            )
            conn.close()

            # Create display names for each project
            projects_df['display_name'] = projects_df.apply(
                lambda
                    row: f"{row['organization']} - {row['entity_name']} - {row['unit_name']} - {row['project_name']} ({row['target_year']}) - {row['project_owner']}",
                axis=1
            )
            projects_df['full_info'] = projects_df.apply(
                lambda row: {
                    'project_code': row['project_code'],
                    'organization': row['organization'],
                    'entity_name': row['entity_name'],
                    'unit_name': row['unit_name'],
                    'project_name': row['project_name'],
                    'target_year': row['target_year'],
                    'project_owner': row['project_owner'],
                    'calculation_method': row['calculation_method']
                },
                axis=1
            )
            return projects_df
        except Exception as e:
            st.warning(f"Could not load projects: {e}")
            return pd.DataFrame(columns=['display_name', 'full_info'])

    # Get organizations for dropdown
    organizations_list = get_organizations_from_fuel_db()

    # --- Control Buttons ---
    st.subheader("Project Management")
    col1, col2, col3, col4, col5 = st.columns([1.5, 1.5, 1.5, 1.5, 1.5])

    if col1.button("🆕 New", key="co2_new", use_container_width=True):
        st.session_state.co2_project = {
            'project_code': '',
            'organization': '',
            'entity_name': '',
            'unit_name': '',
            'project_name': '',
            'base_year': '',
            'target_year': '',
            'implementation_date': '',
            'life_span': '10',
            'project_owner': '',
            'input_data': [{}],
            'output_data': [{}],
            'costing_data': [{}],
            'amp_before': 0.0,
            'amp_after': 0.0,
            'amp_uom': 't/tp',
            'calculation_method': 'absolute',
            'is_loaded_from_db': False,
            'primary_output_before': 0.0,
            'primary_output_after': 0.0,
        }
        st.success("New project created")
        st.rerun()

    # Load project list with search functionality
    projects_df = load_projects_for_dropdown()

    # Create a selectbox with search functionality
    project_options = [""] + projects_df['display_name'].tolist()
    project_data_dict = {display: data for display, data in zip(projects_df['display_name'], projects_df['full_info'])}

    search_col1, search_col2 = st.columns([3, 1])

    with search_col1:
        selected_display_name = st.selectbox(
            "Search and Select Project",
            options=project_options,
            key="co2_load_select",
            help="Type to search projects by organization, entity, unit, project name, target year, or owner"
        )

    with search_col2:
        search_term = st.text_input("Quick Search", placeholder="Search...", key="project_search")

        # Filter options based on search term
        if search_term:
            filtered_options = [opt for opt in project_options if search_term.lower() in opt.lower()]
            if filtered_options:
                selected_display_name = st.selectbox(
                    "Filtered Results",
                    options=filtered_options,
                    key="co2_load_filtered",
                    help="Select from filtered results"
                )
            else:
                st.info("No matching projects found")

    # Get the project data for the selected display name
    selected_project_data = project_data_dict.get(selected_display_name) if selected_display_name else None

    if col3.button("📂 Load", key="co2_load_btn", use_container_width=True):
        if selected_display_name and selected_project_data:
            try:
                project_code = selected_project_data['project_code']
                conn = sqlite3.connect(CO2_DB_PATH)
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM projects WHERE project_code = ?", (project_code,))
                row = cursor.fetchone()

                if row:
                    cols = [description[0] for description in cursor.description]
                    data = dict(zip(cols, row))

                    calc_method = data.get('calculation_method', 'absolute')

                    input_data = parse_json_safely(data.get('input_data'), [{}])
                    output_data = parse_json_safely(data.get('output_data'), [{}])
                    costing_data = parse_json_safely(data.get('costing_data'), [{}])

                    input_data = [normalize_data_row(row) for row in input_data]
                    output_data = [normalize_data_row(row) for row in output_data]
                    costing_data = [normalize_data_row(row) for row in costing_data]

                    if calc_method == 'specific':
                        output_data = enforce_specific_rules(output_data, 'specific', 'output')

                    primary_output_before = 0.0
                    primary_output_after = 0.0
                    if output_data and len(output_data) > 0:
                        if calc_method == 'absolute':
                            primary_output_before = output_data[0].get('abs_before', 0.0)
                            primary_output_after = output_data[0].get('abs_after', 0.0)
                        else:
                            primary_output_before = output_data[0].get('spec_before', 1.0)
                            primary_output_after = output_data[0].get('spec_after', 1.0)

                    new_project = {
                        'project_code': data.get('project_code', ''),
                        'organization': data.get('organization', ''),
                        'entity_name': data.get('entity_name', ''),
                        'unit_name': data.get('unit_name', ''),
                        'project_name': data.get('project_name', ''),
                        'base_year': str(data.get('base_year', '')),
                        'target_year': str(data.get('target_year', '')),
                        'implementation_date': str(data.get('implementation_date', '')),
                        'life_span': str(data.get('life_span', '10')),
                        'project_owner': data.get('project_owner', ''),
                        'input_data': input_data,
                        'output_data': output_data,
                        'costing_data': costing_data,
                        'amp_before': float(data.get('amp_before', 0.0)),
                        'amp_after': float(data.get('amp_after', 0.0)),
                        'amp_uom': data.get('amp_uom', 't/tp'),
                        'calculation_method': calc_method,
                        'is_loaded_from_db': True,
                        'primary_output_before': primary_output_before,
                        'primary_output_after': primary_output_after,
                    }

                    st.session_state.co2_project = new_project
                    st.success(f"✅ Successfully loaded: {selected_display_name}")
                    st.rerun()
                else:
                    st.error("❌ Project not found in database")

                conn.close()
            except Exception as e:
                st.error(f"❌ Load error: {str(e)}")
        else:
            st.warning("⚠️ Please select a project to load")

    if col4.button("💾 Save", key="co2_save", use_container_width=True):
        if not project['project_name'].strip():
            st.error("❌ Project Name is required")
        elif not project['organization'].strip():
            st.error("❌ Organization is required")
        elif not project['entity_name'].strip():
            st.error("❌ Entity Name is required")
        elif not project['unit_name'].strip():
            st.error("❌ Unit Name is required")
        elif not project['target_year'].strip():
            st.error("❌ Target Year is required")
        elif not project['project_owner'].strip():
            st.error("❌ Project Owner is required")
        else:
            if project['calculation_method'] == 'specific':
                project['output_data'] = enforce_specific_rules(project['output_data'], 'specific', 'output')

            if project['output_data'] and len(project['output_data']) > 0:
                if project['calculation_method'] == 'absolute':
                    project['primary_output_before'] = project['output_data'][0].get('abs_before', 0.0)
                    project['primary_output_after'] = project['output_data'][0].get('abs_after', 0.0)
                else:
                    project['primary_output_before'] = project['output_data'][0].get('spec_before', 1.0)
                    project['primary_output_after'] = project['output_data'][0].get('spec_after', 1.0)

            # Generate new Project ID if needed or update existing
            if not project['project_code'] or not project.get('is_loaded_from_db', False):
                project['project_code'] = generate_project_id(
                    project['organization'],
                    project['entity_name'],
                    project['unit_name'],
                    project['project_name'],
                    project['target_year'],
                    project['project_owner']
                )

            try:
                conn = sqlite3.connect(CO2_DB_PATH)
                c = conn.cursor()

                input_json = json.dumps(project['input_data'], default=str)
                output_json = json.dumps(project['output_data'], default=str)
                costing_json = json.dumps(project['costing_data'], default=str)

                # Check if project with this name already exists (for update)
                c.execute("SELECT project_code FROM projects WHERE project_name = ?", (project['project_name'],))
                existing = c.fetchone()

                if existing and existing[0] != project['project_code']:
                    st.error("❌ A project with this name already exists. Please use a different project name.")
                else:
                    c.execute(''' 
                        INSERT OR REPLACE INTO projects 
                        (project_code, organization, entity_name, unit_name, project_name, base_year, target_year,
                         implementation_date, life_span, project_owner, input_data, output_data, costing_data,
                         amp_before, amp_after, amp_uom, calculation_method, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ''', (
                        project['project_code'],
                        project['organization'],
                        project['entity_name'],
                        project['unit_name'],
                        project['project_name'],
                        project['base_year'],
                        project['target_year'],
                        project['implementation_date'],
                        project['life_span'],
                        project['project_owner'],
                        input_json,
                        output_json,
                        costing_json,
                        float(project['amp_before']),
                        float(project['amp_after']),
                        project['amp_uom'],
                        project['calculation_method']
                    ))
                    conn.commit()
                    conn.close()

                    project['is_loaded_from_db'] = True

                    # Show the generated Project ID
                    st.success(f"✅ Project saved successfully!")
                    st.info(f"**Project ID:** {project['project_code']}")
                    st.balloons()
                    st.rerun()

            except sqlite3.IntegrityError as e:
                if "UNIQUE constraint failed" in str(e):
                    st.error("❌ A project with this Project ID already exists. Please generate a new one.")
                else:
                    st.error(f"❌ Database error: {str(e)}")
            except Exception as e:
                st.error(f"❌ Save error: {str(e)}")

    # FIXED DELETE BUTTON - NO CONFIRMATION NEEDED
    if col5.button("🗑️ Delete", key="co2_delete", use_container_width=True):
        if selected_display_name and selected_project_data:
            project_code_to_delete = selected_project_data['project_code']
            try:
                conn = sqlite3.connect(CO2_DB_PATH)

                # First delete related tracking data
                conn.execute("DELETE FROM project_actuals WHERE project_code = ?", (project_code_to_delete,))
                conn.execute("DELETE FROM amp_actuals_tracking WHERE project_code = ?", (project_code_to_delete,))

                # Then delete the project
                conn.execute("DELETE FROM projects WHERE project_code = ?", (project_code_to_delete,))
                conn.commit()
                conn.close()

                st.success(f"✅ Deleted: {selected_display_name}")

                # Clear current project if it matches
                if project['project_code'] == project_code_to_delete:
                    st.session_state.co2_project = {
                        'project_code': '',
                        'organization': '',
                        'entity_name': '',
                        'unit_name': '',
                        'project_name': '',
                        'base_year': '',
                        'target_year': '',
                        'implementation_date': '',
                        'life_span': '10',
                        'project_owner': '',
                        'input_data': [{}],
                        'output_data': [{}],
                        'costing_data': [{}],
                        'amp_before': 0.0,
                        'amp_after': 0.0,
                        'amp_uom': 't/tp',
                        'calculation_method': 'absolute',
                        'is_loaded_from_db': False,
                        'primary_output_before': 0.0,
                        'primary_output_after': 0.0,
                    }

                # Force refresh by rerunning
                st.rerun()

            except Exception as e:
                st.error(f"❌ Delete error: {str(e)}")
        else:
            st.warning("⚠️ Please select a project to delete")

    # Display current project info
    if project['project_code']:
        method_status = "🔒 FIXED" if project.get('is_loaded_from_db', False) else "✏️ EDITABLE"
        st.info(
            f"**Current Project:** {project['project_name']} ({project['project_code']}) - {project['calculation_method'].upper()} method - {method_status}")

        # Show Project ID details
        with st.expander("📋 Project ID Details", expanded=False):
            st.write(f"""
            **Project ID Components:**
            - **Organization:** {project['organization']}
            - **Entity:** {project['entity_name']}
            - **Unit:** {project['unit_name']}
            - **Project Name:** {project['project_name']}
            - **Target Year:** {project['target_year']}
            - **Project Owner:** {project['project_owner']}
            """)

    # --- General Information (Narrow Fields) ---
    st.subheader("📝 General Information")
    g1, g2, g3, g4, g5 = st.columns(5)

    with g1:
        st.markdown("**Organization**")
        if organizations_list:
            current_org = project['organization'] if project['organization'] in organizations_list else ""
            selected_org = st.selectbox(
                "",
                options=[""] + organizations_list,
                index=organizations_list.index(current_org) + 1 if current_org in organizations_list else 0,
                label_visibility="collapsed",
                key="co2_org_dropdown"
            )
            project['organization'] = selected_org
        else:
            project['organization'] = st.text_input("", value=project['organization'], label_visibility="collapsed",
                                                    key="co2_org_text")

        st.markdown("**Entity Name**")
        project['entity_name'] = st.text_input("", value=project['entity_name'], label_visibility="collapsed",
                                               key="co2_entity")

    with g2:
        st.markdown("**Unit Name**")
        project['unit_name'] = st.text_input("", value=project['unit_name'], label_visibility="collapsed",
                                             key="co2_unit")
        st.markdown("**Project Name**")
        project['project_name'] = st.text_input("", value=project['project_name'], label_visibility="collapsed",
                                                key="co2_proj_name")

    with g3:
        st.markdown("**Base Year**")
        project['base_year'] = st.text_input("", value=project['base_year'], label_visibility="collapsed",
                                             key="co2_base_year")
        st.markdown("**Target Year**")
        project['target_year'] = st.text_input("", value=project['target_year'], label_visibility="collapsed",
                                               key="co2_target_year")

    with g4:
        st.markdown("**Implementation Date**")
        if project['implementation_date']:
            try:
                date_obj = datetime.strptime(project['implementation_date'], '%Y-%m-%d')
            except:
                date_obj = datetime.today()
        else:
            date_obj = datetime.today()

        selected_date = st.date_input(
            "",
            value=date_obj,
            label_visibility="collapsed",
            key="co2_impl_date_picker"
        )
        project['implementation_date'] = selected_date.strftime('%Y-%m-%d')

        st.markdown("**Life Span (Years)**")
        project['life_span'] = st.text_input("", value=project['life_span'], label_visibility="collapsed",
                                             key="co2_life_span")

    with g5:
        st.markdown("**Project Owner**")
        project['project_owner'] = st.text_input("", value=project['project_owner'], label_visibility="collapsed",
                                                 key="co2_owner")

    # --- Calculation Method ---
    st.subheader("🔢 Calculation Method")

    is_loaded_from_db = project.get('is_loaded_from_db', False)

    if is_loaded_from_db:
        st.info(
            f"**Calculation Method:** {project['calculation_method'].upper()} (Locked - cannot change for saved projects)")

        if project['calculation_method'] == 'absolute':
            st.radio(
                "Select Calculation Method:",
                ["absolute", "specific"],
                index=0,
                horizontal=True,
                key="co2_calc_method_display",
                disabled=True
            )
        else:
            st.radio(
                "Select Calculation Method:",
                ["absolute", "specific"],
                index=1,
                horizontal=True,
                key="co2_calc_method_display",
                disabled=True
            )

        calc_method = project['calculation_method']
    else:
        calc_method = st.radio(
            "Select Calculation Method:",
            ["absolute", "specific"],
            index=0 if project['calculation_method'] == 'absolute' else 1,
            horizontal=True,
            key="co2_calc_method"
        )
        project['calculation_method'] = calc_method

    is_absolute = calc_method == 'absolute'
    is_specific = calc_method == 'specific'

    if not is_loaded_from_db and is_specific:
        project['output_data'] = enforce_specific_rules(project['output_data'], 'specific', 'output')

    # --- Table Renderer ---
    def render_emission_table(title, data_key, is_output=False):
        st.subheader(f"📋 {title}")

        base_headers = ["Material", "UOM", "Emission Factor (tCO₂e/unit)"]
        if is_absolute:
            headers = base_headers + ["Absolute Before", "Absolute After"]
        elif is_specific:
            headers = base_headers + ["Specific Before", "Specific After"]

        header_cols = st.columns(len(headers) + 1)
        for idx, h in enumerate(headers):
            header_cols[idx].markdown(f"**{h}**")
        header_cols[-1].markdown("**Action**")

        updated_data = []
        current_data = project.get(data_key, [{}])

        for i in range(len(current_data)):
            row = current_data[i] if i < len(current_data) else {}
            c = st.columns(len(headers) + 1)
            row_dict = {}

            material = row.get('material', '')
            uom = row.get('uom', '')
            ef = float(row.get('ef', 0.0))

            # Material field - special handling for output data
            if is_output and i == 0:
                # First output row is fixed as "Main Output"
                row_dict['material'] = "Main Output"
                c[0].text_input("", value="Main Output", disabled=True, label_visibility="collapsed",
                                key=f"{data_key}_mat_{i}")
            else:
                row_dict['material'] = c[0].text_input("", value=material, label_visibility="collapsed",
                                                       key=f"{data_key}_mat_{i}")

            row_dict['uom'] = c[1].text_input("", value=uom, label_visibility="collapsed", key=f"{data_key}_uom_{i}")
            row_dict['ef'] = c[2].number_input("", value=ef, format="%.6f", label_visibility="collapsed",
                                               key=f"{data_key}_ef_{i}")

            col_idx = 3

            if is_absolute:
                abs_before = float(row.get('abs_before', 0.0))
                abs_after = float(row.get('abs_after', 0.0))

                row_dict['abs_before'] = c[col_idx].number_input(
                    "", value=abs_before, label_visibility="collapsed", key=f"{data_key}_abs_b_{i}"
                )
                col_idx += 1
                row_dict['abs_after'] = c[col_idx].number_input(
                    "", value=abs_after, label_visibility="collapsed", key=f"{data_key}_abs_a_{i}"
                )
                col_idx += 1

            if is_specific:
                spec_before = float(row.get('spec_before', 0.0))
                spec_after = float(row.get('spec_after', 0.0))

                if is_output:
                    row_dict['abs_before'] = 1.0
                    spec_before = 1.0
                    spec_after = 1.0

                if is_output:
                    row_dict['spec_before'] = 1.0
                    c[col_idx].number_input(
                        "", value=1.0, disabled=True, label_visibility="collapsed", key=f"{data_key}_spec_b_{i}"
                    )
                else:
                    row_dict['spec_before'] = c[col_idx].number_input(
                        "", value=spec_before, label_visibility="collapsed", key=f"{data_key}_spec_b_{i}"
                    )
                col_idx += 1

                if is_output:
                    row_dict['spec_after'] = 1.0
                    c[col_idx].number_input(
                        "", value=1.0, disabled=True, label_visibility="collapsed", key=f"{data_key}_spec_a_{i}"
                    )
                else:
                    row_dict['spec_after'] = c[col_idx].number_input(
                        "", value=spec_after, label_visibility="collapsed", key=f"{data_key}_spec_a_{i}"
                    )
                col_idx += 1

            # Delete button - protect first row for output data
            delete_disabled = (i == 0) and (data_key in ['input_data', 'output_data'])
            if c[-1].button("❌", key=f"delete_{data_key}_{i}", disabled=delete_disabled):
                if i > 0:
                    project[data_key].pop(i)
                    st.rerun()

            updated_data.append(row_dict)

        project[data_key] = updated_data

        if st.button(f"➕ Add Row", key=f"add_row_{data_key}", use_container_width=True):
            new_row = {'material': '', 'uom': '', 'ef': 0.0}
            if is_absolute:
                new_row['abs_before'] = 0.0
                new_row['abs_after'] = 0.0
            if is_specific:
                if data_key == 'output_data' and is_output:
                    new_row['abs_before'] = 1.0
                    new_row['spec_before'] = 1.0
                    new_row['spec_after'] = 1.0
                else:
                    new_row['spec_before'] = 0.0
                    new_row['spec_after'] = 0.0
            project[data_key].append(new_row)
            st.rerun()

    # Input & Output Tables
    render_emission_table("Input Data", "input_data", is_output=False)
    render_emission_table("Output Data", "output_data", is_output=True)

    # --- AMP (only in Specific mode) ---
    if is_specific:
        st.subheader("🏭 Annual Material Production (AMP)")
        a1, a2, a3 = st.columns(3)
        project['amp_before'] = a1.number_input("AMP Before", value=project['amp_before'], key="co2_amp_before")
        project['amp_after'] = a2.number_input("AMP After", value=project['amp_after'], key="co2_amp_after")
        project['amp_uom'] = a3.selectbox(
            "UOM",
            ["t/tp", "kl/tp", "kWh/tp", "kg/tp", "tons/tp"],
            index=["t/tp", "kl/tp", "kWh/tp", "kg/tp", "tons/tp"].index(project['amp_uom']) if project['amp_uom'] in [
                "t/tp", "kl/tp", "kWh/tp", "kg/tp", "tons/tp"] else 0,
            key="co2_amp_uom"
        )
    elif is_loaded_from_db and project.get('calculation_method', '') == 'absolute':
        pass

    # --- Costing Table (No EF) ---
    st.subheader("💰 Costing Data")

    def render_costing_table():
        costing_headers = ["Particular", "UOM"]
        if is_absolute:
            costing_headers += ["Absolute Before", "Absolute After"]
        if is_specific:
            costing_headers += ["Specific Before", "Specific After"]

        header_cols = st.columns(len(costing_headers) + 1)
        for idx, h in enumerate(costing_headers):
            header_cols[idx].markdown(f"**{h}**")
        header_cols[-1].markdown("**Action**")

        updated_costing = []
        current_costing = project.get('costing_data', [{}])

        # Ensure we have at least 3 rows for costing data
        while len(current_costing) < 3:
            current_costing.append({})

        for i in range(len(current_costing)):
            row = current_costing[i] if i < len(current_costing) else {}
            c = st.columns(len(costing_headers) + 1)
            row_dict = {}

            # Special handling for first three rows
            if i == 0:
                # First row: CAPEX (fixed)
                row_dict['material'] = "CAPEX"
                c[0].text_input("", value="CAPEX", disabled=True, label_visibility="collapsed",
                                key=f"cost_mat_{i}")
            elif i == 1:
                # Second row: OPEX-Only Fuel/Energy (fixed)
                row_dict['material'] = "OPEX-Only Fuel/Energy"
                c[0].text_input("", value="OPEX-Only Fuel/Energy", disabled=True, label_visibility="collapsed",
                                key=f"cost_mat_{i}")
            elif i == 2:
                # Third row: OPEX-Other than Fuel/Energy (fixed)
                row_dict['material'] = "OPEX-Other than Fuel/Energy"
                c[0].text_input("", value="OPEX-Other than Fuel/Energy", disabled=True, label_visibility="collapsed",
                                key=f"cost_mat_{i}")
            else:
                # Other rows: editable
                row_dict['material'] = c[0].text_input("", value=row.get('material', ''), label_visibility="collapsed",
                                                       key=f"cost_mat_{i}")

            row_dict['uom'] = c[1].text_input("", value=row.get('uom', ''), label_visibility="collapsed",
                                              key=f"cost_uom_{i}")

            col_idx = 2

            if is_absolute:
                abs_before = float(row.get('abs_before', 0.0))
                abs_after = float(row.get('abs_after', 0.0))

                row_dict['abs_before'] = c[col_idx].number_input(
                    "", value=abs_before, label_visibility="collapsed", key=f"cost_abs_b_{i}"
                )
                col_idx += 1
                row_dict['abs_after'] = c[col_idx].number_input(
                    "", value=abs_after, label_visibility="collapsed", key=f"cost_abs_a_{i}"
                )
                col_idx += 1

            if is_specific:
                spec_before = float(row.get('spec_before', 0.0))
                spec_after = float(row.get('spec_after', 0.0))

                row_dict['spec_before'] = c[col_idx].number_input(
                    "", value=spec_before, label_visibility="collapsed", key=f"cost_spec_b_{i}"
                )
                col_idx += 1
                row_dict['spec_after'] = c[col_idx].number_input(
                    "", value=spec_after, label_visibility="collapsed", key=f"cost_spec_a_{i}"
                )
                col_idx += 1

            # Delete button - protect first 3 rows
            if c[-1].button("❌", key=f"delete_cost_{i}", disabled=(i < 3)):
                if i >= 3:
                    project['costing_data'].pop(i)
                    st.rerun()

            updated_costing.append(row_dict)

        project['costing_data'] = updated_costing

        # Ensure minimum 3 rows for costing
        while len(project['costing_data']) < 3:
            project['costing_data'].append({'material': '', 'uom': ''})
            if is_absolute:
                project['costing_data'][-1]['abs_before'] = 0.0
                project['costing_data'][-1]['abs_after'] = 0.0
            if is_specific:
                project['costing_data'][-1]['spec_before'] = 0.0
                project['costing_data'][-1]['spec_after'] = 0.0

        # Add row button
        if st.button("➕ Add Row - Costing", key="add_row_costing", use_container_width=True):
            new_row = {'material': '', 'uom': ''}
            if is_absolute:
                new_row['abs_before'] = 0.0
                new_row['abs_after'] = 0.0
            if is_specific:
                new_row['spec_before'] = 0.0
                new_row['spec_after'] = 0.0
            project['costing_data'].append(new_row)
            st.rerun()

    render_costing_table()

    # --- ALWAYS SHOW RESULTS SECTION ---
    st.subheader("📊 Emission Results")

    # Validate data
    if not project['input_data']:
        st.info("ℹ️ Add input data to see results")
        return

    if not project['output_data']:
        st.info("ℹ️ Add output data to see results")
        return

    # Calculate emissions
    amp_before = float(project['amp_before']) if is_specific else 1.0
    amp_after = float(project['amp_after']) if is_specific else 1.0

    def calculate_emission(section_data, section_name="data"):
        total_before = total_after = 0.0

        for row in section_data:
            ef = float(row.get('ef', 0.0))

            if is_absolute:
                abs_before = float(row.get('abs_before', 0.0))
                abs_after = float(row.get('abs_after', 0.0))

                total_before += abs_before * ef
                total_after += abs_after * ef

            else:
                spec_before = float(row.get('spec_before', 0.0))
                spec_after = float(row.get('spec_after', 0.0))

                if section_name == 'output':
                    spec_before = 1.0
                    spec_after = 1.0

                total_before += (amp_before * spec_before) * ef
                total_after += (amp_after * spec_after) * ef

        return total_before, total_after

    try:
        input_before, input_after = calculate_emission(project['input_data'], 'input')
        output_before, output_after = calculate_emission(project['output_data'], 'output')

        net_before = input_before - output_before
        net_after = input_after - output_after

        primary_output_before = 0.0
        primary_output_after = 0.0

        if project['output_data'] and len(project['output_data']) > 0:
            first_output = project['output_data'][0]
            if is_absolute:
                primary_output_before = float(first_output.get('abs_before', 0.0))
                primary_output_after = float(first_output.get('abs_after', 0.0))
            else:
                primary_output_before = 1.0
                primary_output_after = 1.0

        project['primary_output_before'] = primary_output_before
        project['primary_output_after'] = primary_output_after

        sp_net_before = 0.0
        sp_net_after = 0.0
        co2_reduction = 0.0

        if is_absolute:
            if primary_output_before != 0:
                sp_net_before = net_before / primary_output_before
            if primary_output_after != 0:
                sp_net_after = net_after / primary_output_after

            co2_reduction = (sp_net_before - sp_net_after) * primary_output_after

        else:
            if amp_before != 0:
                sp_net_before = net_before / amp_before
            if amp_after != 0:
                sp_net_after = net_after / amp_after

            co2_reduction = (sp_net_before - sp_net_after) * amp_after

        input_net_change = input_before - input_after
        output_net_change = output_before - output_after
        net_net_change = net_before - net_after
        sp_net_net_change = sp_net_before - sp_net_after

        baseline_values = {
            'input_before': input_before,
            'input_after': input_after,
            'output_before': output_before,
            'output_after': output_after,
            'net_before': net_before,
            'net_after': net_after,
            'sp_net_before': sp_net_before,
            'sp_net_after': sp_net_after,
            'co2_reduction': co2_reduction,
            'primary_output_before': primary_output_before,
            'primary_output_after': primary_output_after,
            'amp_before': amp_before,
            'amp_after': amp_after
        }

        results_data = {
            "Parameter": ["Input CO₂", "Output CO₂", "Net CO₂", "Sp.Net (tCO₂e/unit)", "CO₂ Reduction"],
            "Before": [
                f"{input_before:,.2f}",
                f"{output_before:,.2f}",
                f"{net_before:,.2f}",
                f"{sp_net_before:,.6f}",
                ""
            ],
            "After": [
                f"{input_after:,.2f}",
                f"{output_after:,.2f}",
                f"{net_after:,.2f}",
                f"{sp_net_after:,.6f}",
                f"{co2_reduction:,.2f}"
            ],
            "Net Change": [
                f"{input_net_change:,.2f}",
                f"{output_net_change:,.2f}",
                f"{net_net_change:,.2f}",
                f"{sp_net_net_change:,.6f}",
                f"{co2_reduction:,.2f}"
            ]
        }

        results_df = pd.DataFrame(results_data)

        st.dataframe(
            results_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Parameter": st.column_config.TextColumn("Parameter", width="medium"),
                "Before": st.column_config.TextColumn("Before", width="medium"),
                "After": st.column_config.TextColumn("After", width="medium"),
                "Net Change": st.column_config.TextColumn("Net Change", width="medium")
            }
        )

        if is_absolute:
            st.info(f"""
            **Absolute Method Calculation Details:**
            - **Sp.Net_Before** = Net CO₂_Before / Primary Output_Before = {net_before:,.2f} / {primary_output_before:,.2f} = {sp_net_before:,.6f} tCO₂e/unit
            - **Sp.Net_After** = Net CO₂_After / Primary Output_After = {net_after:,.2f} / {primary_output_after:,.2f} = {sp_net_after:,.6f} tCO₂e/unit
            - **CO₂ Reduction** = (Sp.Net_Before - Sp.Net_After) × Primary Output_After = ({sp_net_before:,.6f} - {sp_net_after:,.6f}) × {primary_output_after:,.2f} = {co2_reduction:,.2f} tCO₂e
            """)
        else:
            st.info(f"""
            **Specific Method Calculation Details:**
            - **Sp.Net_Before** = Net CO₂_Before / AMP_Before = {net_before:,.2f} / {amp_before:,.2f} = {sp_net_before:,.6f} tCO₂e/unit
            - **Sp.Net_After** = Net CO₂_After / AMP_After = {net_after:,.2f} / {amp_after:,.2f} = {sp_net_after:,.6f} tCO₂e/unit
            - **CO₂ Reduction** = (Sp.Net_Before - Sp.Net_After) × AMP_After = ({sp_net_before:,.6f} - {sp_net_after:,.6f}) × {amp_after:,.2f} = {co2_reduction:,.2f} tCO₂e
            """)

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total CO₂ Reduction", f"{co2_reduction:,.2f} tCO₂e",
                    delta=f"{co2_reduction:,.0f} tCO₂e" if co2_reduction > 0 else f"{co2_reduction:,.0f} tCO₂e",
                    delta_color="normal" if co2_reduction > 0 else "inverse")

        if net_before > 0:
            reduction_pct = (co2_reduction / net_before * 100)
        else:
            reduction_pct = 0.0

        col2.metric("Reduction %", f"{reduction_pct:.1f}%",
                    delta=f"{reduction_pct:.1f}%" if reduction_pct > 0 else f"{reduction_pct:.1f}%",
                    delta_color="normal" if reduction_pct > 0 else "inverse")

        col3.metric("Sp.Net Improvement", f"{sp_net_net_change:,.6f} tCO₂e/unit",
                    delta=f"{sp_net_net_change:,.6f}" if sp_net_net_change > 0 else f"{sp_net_net_change:,.6f}",
                    delta_color="normal" if sp_net_net_change > 0 else "inverse")

        col4.metric("Method Used", project['calculation_method'].upper())

        fig = go.Figure()

        fig.add_trace(go.Bar(
            name='Before',
            x=['Input CO₂', 'Output CO₂', 'Net CO₂', 'Sp.Net'],
            y=[input_before, output_before, net_before, sp_net_before * 10000],
            marker_color='blue'
        ))

        fig.add_trace(go.Bar(
            name='After',
            x=['Input CO₂', 'Output CO₂', 'Net CO₂', 'Sp.Net'],
            y=[input_after, output_after, net_after, sp_net_after * 10000],
            marker_color='green'
        ))

        fig.add_trace(go.Scatter(
            name='CO₂ Reduction',
            x=['CO₂ Reduction'],
            y=[co2_reduction],
            mode='markers+text',
            marker=dict(
                size=15,
                color='red',
                symbol='diamond'
            ),
            text=[f"{co2_reduction:,.0f}"],
            textposition='top center',
            showlegend=True
        ))

        fig.update_layout(
            title='CO₂ Emissions Analysis with Sp.Net and Reduction',
            barmode='group',
            yaxis_title='tCO₂e (Sp.Net × 10,000)',
            template='plotly_white',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            annotations=[
                dict(
                    x='Sp.Net',
                    y=sp_net_before * 10000,
                    text=f"Sp.Net_Before: {sp_net_before:.6f}",
                    showarrow=True,
                    arrowhead=2,
                    ax=0,
                    ay=-40
                ),
                dict(
                    x='Sp.Net',
                    y=sp_net_after * 10000,
                    text=f"Sp.Net_After: {sp_net_after:.6f}",
                    showarrow=True,
                    arrowhead=2,
                    ax=0,
                    ay=40
                )
            ]
        )
        st.plotly_chart(fig, use_container_width=True)

        fig_reduction = go.Figure()

        if is_absolute:
            fig_reduction.add_trace(go.Bar(
                name='Sp.Net_Before',
                x=['Sp.Net Before'],
                y=[sp_net_before],
                marker_color='blue',
                text=[f"{sp_net_before:.6f}"],
                textposition='auto'
            ))

            fig_reduction.add_trace(go.Bar(
                name='Sp.Net_After',
                x=['Sp.Net After'],
                y=[sp_net_after],
                marker_color='green',
                text=[f"{sp_net_after:.6f}"],
                textposition='auto'
            ))

            fig_reduction.add_trace(go.Bar(
                name='Primary Output After',
                x=['Primary Output'],
                y=[primary_output_after],
                marker_color='orange',
                text=[f"{primary_output_after:,.0f}"],
                textposition='auto'
            ))

            fig_reduction.update_layout(
                title='CO₂ Reduction Calculation Components (Absolute Method)',
                yaxis_title='Value',
                template='plotly_white',
                showlegend=True
            )

        else:
            fig_reduction.add_trace(go.Bar(
                name='Sp.Net_Before',
                x=['Sp.Net Before'],
                y=[sp_net_before],
                marker_color='blue',
                text=[f"{sp_net_before:.6f}"],
                textposition='auto'
            ))

            fig_reduction.add_trace(go.Bar(
                name='Sp.Net_After',
                x=['Sp.Net After'],
                y=[sp_net_after],
                marker_color='green',
                text=[f"{sp_net_after:.6f}"],
                textposition='auto'
            ))

            fig_reduction.add_trace(go.Bar(
                name='AMP After',
                x=['AMP'],
                y=[amp_after],
                marker_color='purple',
                text=[f"{amp_after:,.0f}"],
                textposition='auto'
            ))

            fig_reduction.update_layout(
                title='CO₂ Reduction Calculation Components (Specific Method)',
                yaxis_title='Value',
                template='plotly_white',
                showlegend=True
            )

        st.plotly_chart(fig_reduction, use_container_width=True)

    except Exception as e:
        st.error(f"❌ Calculation error: {str(e)}")
        import traceback
        st.error(f"Traceback: {traceback.format_exc()}")
        st.info("Please check that all numeric fields have valid values.")

    # --- Tracking Section with Summary Table ---
    with st.expander("📈 Track Project Actuals", expanded=False):
        if project['project_code']:
            try:
                life_span = int(project['life_span']) if project['life_span'].isdigit() else 10

                conn = sqlite3.connect(CO2_DB_PATH)
                cursor = conn.cursor()

                # Store tracking data for calculations
                year_data_store = {}

                # Check if we have stored baseline calculations
                baseline_values = {}
                if 'emission_results_calculated' in project:
                    baseline_values = project['emission_results_calculated']
                else:
                    # Calculate baseline values from project data
                    amp_before = float(project['amp_before']) if is_specific else 1.0
                    amp_after = float(project['amp_after']) if is_specific else 1.0

                    # Calculate "After" values from Emission Results
                    def calculate_after_values():
                        total_input_after = 0.0
                        total_output_after = 0.0

                        # Calculate input emissions (After)
                        for row in project['input_data']:
                            ef = float(row.get('ef', 0.0))
                            if is_absolute:
                                abs_after = float(row.get('abs_after', 0.0))
                                total_input_after += abs_after * ef
                            else:
                                spec_after = float(row.get('spec_after', 0.0))
                                total_input_after += (amp_after * spec_after) * ef

                        # Calculate output emissions (After)
                        for row in project['output_data']:
                            ef = float(row.get('ef', 0.0))
                            if is_absolute:
                                abs_after = float(row.get('abs_after', 0.0))
                                total_output_after += abs_after * ef
                            else:
                                # For output in specific mode, spec_after is always 1
                                total_output_after += (amp_after * 1.0) * ef

                        net_after = total_input_after - total_output_after

                        # Calculate Sp.Net (After)
                        if is_absolute:
                            if project['output_data'] and len(project['output_data']) > 0:
                                primary_output_after = float(project['output_data'][0].get('abs_after', 0.0))
                                sp_net_after = net_after / primary_output_after if primary_output_after != 0 else 0.0
                            else:
                                sp_net_after = 0.0
                        else:  # specific
                            sp_net_after = net_after / amp_after if amp_after != 0 else 0.0

                        # Calculate CO₂ Reduction (Net Change value)
                        # First calculate Before values
                        total_input_before = 0.0
                        total_output_before = 0.0

                        # Calculate input emissions (Before)
                        for row in project['input_data']:
                            ef = float(row.get('ef', 0.0))
                            if is_absolute:
                                abs_before = float(row.get('abs_before', 0.0))
                                total_input_before += abs_before * ef
                            else:
                                spec_before = float(row.get('spec_before', 0.0))
                                total_input_before += (amp_before * spec_before) * ef

                        # Calculate output emissions (Before)
                        for row in project['output_data']:
                            ef = float(row.get('ef', 0.0))
                            if is_absolute:
                                abs_before = float(row.get('abs_before', 0.0))
                                total_output_before += abs_before * ef
                            else:
                                # For output in specific mode, spec_before is always 1
                                total_output_before += (amp_before * 1.0) * ef

                        net_before = total_input_before - total_output_before

                        # Calculate Sp.Net (Before)
                        if is_absolute:
                            if project['output_data'] and len(project['output_data']) > 0:
                                primary_output_before = float(project['output_data'][0].get('abs_before', 0.0))
                                sp_net_before = net_before / primary_output_before if primary_output_before != 0 else 0.0
                            else:
                                sp_net_before = 0.0
                        else:  # specific
                            sp_net_before = net_before / amp_before if amp_before != 0 else 0.0

                        # Calculate CO₂ Reduction
                        if is_absolute:
                            co2_reduction = (sp_net_before - sp_net_after) * primary_output_after
                        else:  # specific
                            co2_reduction = (sp_net_before - sp_net_after) * amp_after

                        return {
                            'input': total_input_after,
                            'output': total_output_after,
                            'net': net_after,
                            'sp_net': sp_net_after,
                            'co2_reduction': co2_reduction,
                            'primary_output_after': primary_output_after if is_absolute else amp_after,
                            'sp_net_before': sp_net_before
                        }

                    baseline_values = calculate_after_values()
                    # Store for future use
                    project['emission_results_calculated'] = baseline_values

                # Get existing tracking years
                cursor.execute(''' 
                    SELECT DISTINCT year_number 
                    FROM project_actuals 
                    WHERE project_code = ? 
                    UNION 
                    SELECT DISTINCT year_number 
                    FROM amp_actuals_tracking 
                    WHERE project_code = ?
                    ORDER BY year_number
                ''', (project['project_code'], project['project_code']))

                existing_years = [row[0] for row in cursor.fetchall()]
                all_years = list(range(1, life_span + 1)) + existing_years
                all_years = sorted(set(all_years))

                # Create tabs for each year
                if all_years:
                    year_tabs = st.tabs([f"Year {y}" for y in all_years])

                    for y_idx, tab in enumerate(year_tabs):
                        with tab:
                            year = all_years[y_idx]
                            st.markdown(f"### 📅 Enter Data for Year {year}")

                            # Load existing values
                            cursor.execute(''' 
                                SELECT section_type, material_name, row_index, absolute_value, specific_value 
                                FROM project_actuals 
                                WHERE project_code = ? AND year_number = ?
                            ''', (project['project_code'], year))

                            existing_values = {}
                            for section, material, row_idx, abs_val, spec_val in cursor.fetchall():
                                key = f"{section}_{row_idx}"
                                existing_values[key] = {'abs': abs_val, 'spec': spec_val}

                            # Load existing AMP
                            cursor.execute(''' 
                                SELECT amp_value FROM amp_actuals_tracking 
                                WHERE project_code = ? AND year_number = ?
                            ''', (project['project_code'], year))

                            amp_row = cursor.fetchone()
                            default_amp = amp_row[0] if amp_row else 0.0

                            # Track current year values
                            current_input_values = []
                            current_output_values = []
                            current_amp_value = 0.0

                            # Get EF values from project data
                            input_ef_list = [float(row.get('ef', 0.0)) for row in project.get('input_data', [])]
                            output_ef_list = [float(row.get('ef', 0.0)) for row in project.get('output_data', [])]

                            # Input Data Entry
                            st.markdown("**Input Data**")
                            input_data_updated = False

                            for idx, row in enumerate(project.get('input_data', [])):
                                material = row.get('material', f'Input {idx + 1}')
                                key = f"input_{idx}"

                                default_abs = existing_values.get(key, {}).get('abs', 0.0)
                                default_spec = existing_values.get(key, {}).get('spec', 0.0)

                                col1, col2 = st.columns([3, 1])

                                if is_absolute:
                                    # For absolute calculations: Enter Absolute value
                                    with col1:
                                        abs_val = st.number_input(
                                            f"{material} - Absolute Value",
                                            value=float(default_abs),
                                            step=0.01,
                                            key=f"track_in_abs_{idx}_y{year}"
                                        )
                                        current_input_values.append(abs_val)

                                    with col2:
                                        if st.button(f"💾", key=f"save_in_{idx}_y{year}", help=f"Save {material}"):
                                            cursor.execute(''' 
                                                INSERT OR REPLACE INTO project_actuals 
                                                (project_code, section_type, material_name, row_index, year_number, absolute_value, specific_value)
                                                VALUES (?, 'input', ?, ?, ?, ?, ?)
                                            ''', (
                                                project['project_code'],
                                                material,
                                                idx,
                                                year,
                                                abs_val,
                                                None  # Specific value is None for absolute calculations
                                            ))
                                            conn.commit()
                                            input_data_updated = True
                                else:
                                    # For specific calculations: Enter Specific value
                                    with col1:
                                        spec_val = st.number_input(
                                            f"{material} - Specific Value",
                                            value=float(default_spec),
                                            step=0.01,
                                            key=f"track_in_spec_{idx}_y{year}"
                                        )
                                        current_input_values.append(spec_val)

                                    with col2:
                                        if st.button(f"💾", key=f"save_in_{idx}_y{year}", help=f"Save {material}"):
                                            cursor.execute(''' 
                                                INSERT OR REPLACE INTO project_actuals 
                                                (project_code, section_type, material_name, row_index, year_number, absolute_value, specific_value)
                                                VALUES (?, 'input', ?, ?, ?, ?, ?)
                                            ''', (
                                                project['project_code'],
                                                material,
                                                idx,
                                                year,
                                                None,  # Absolute value is None for specific calculations
                                                spec_val
                                            ))
                                            conn.commit()
                                            input_data_updated = True

                            # Output Data Entry
                            st.markdown("**Output Data**")
                            output_data_updated = False

                            for idx, row in enumerate(project.get('output_data', [])):
                                material = row.get('material', f'Output {idx + 1}')
                                key = f"output_{idx}"

                                default_abs = existing_values.get(key, {}).get('abs', 0.0)
                                default_spec = existing_values.get(key, {}).get('spec', 0.0)

                                col1, col2 = st.columns([3, 1])

                                if is_absolute:
                                    # For absolute calculations: Enter Absolute value
                                    with col1:
                                        abs_val = st.number_input(
                                            f"{material} - Absolute Value",
                                            value=float(default_abs),
                                            step=0.01,
                                            key=f"track_out_abs_{idx}_y{year}"
                                        )
                                        current_output_values.append(abs_val)

                                    with col2:
                                        if st.button(f"💾", key=f"save_out_{idx}_y{year}", help=f"Save {material}"):
                                            cursor.execute(''' 
                                                INSERT OR REPLACE INTO project_actuals 
                                                (project_code, section_type, material_name, row_index, year_number, absolute_value, specific_value)
                                                VALUES (?, 'output', ?, ?, ?, ?, ?)
                                            ''', (
                                                project['project_code'],
                                                material,
                                                idx,
                                                year,
                                                abs_val,
                                                None  # Specific value is None for absolute calculations
                                            ))
                                            conn.commit()
                                            output_data_updated = True
                                else:
                                    # For output in specific mode, values are fixed at 1
                                    with col1:
                                        st.text_input(
                                            f"{material} - Specific Value",
                                            value="1.00",
                                            disabled=True,
                                            key=f"track_out_spec_{idx}_y{year}"
                                        )
                                        current_output_values.append(1.0)  # Fixed at 1 for specific

                                    with col2:
                                        if st.button(f"💾", key=f"save_out_{idx}_y{year}", help=f"Save {material}"):
                                            cursor.execute(''' 
                                                INSERT OR REPLACE INTO project_actuals 
                                                (project_code, section_type, material_name, row_index, year_number, absolute_value, specific_value)
                                                VALUES (?, 'output', ?, ?, ?, ?, ?)
                                            ''', (
                                                project['project_code'],
                                                material,
                                                idx,
                                                year,
                                                1.0 if idx == 0 else None,  # Absolute Before = 1 for first output row
                                                1.0  # Specific After = 1
                                            ))
                                            conn.commit()
                                            output_data_updated = True

                            # AMP Entry (only for specific calculations)
                            amp_updated = False
                            if is_specific:
                                st.markdown("**Annual Material Production (AMP)**")

                                col1, col2 = st.columns([3, 1])
                                with col1:
                                    amp_val = st.number_input(
                                        f"AMP Value ({project['amp_uom']})",
                                        value=float(default_amp),
                                        step=0.01,
                                        key=f"track_amp_y{year}"
                                    )
                                    current_amp_value = amp_val

                                with col2:
                                    if st.button(f"💾 AMP", key=f"save_amp_y{year}", help="Save AMP"):
                                        cursor.execute(''' 
                                            INSERT OR REPLACE INTO amp_actuals_tracking 
                                            (project_code, year_number, amp_value)
                                            VALUES (?, ?, ?)
                                        ''', (project['project_code'], year, amp_val))
                                        conn.commit()
                                        amp_updated = True
                            else:
                                # For absolute calculations, AMP is not applicable
                                current_amp_value = 1.0
                                st.info("AMP tracking is only applicable for specific calculations.")

                            # Show save success messages
                            if input_data_updated or output_data_updated or amp_updated:
                                st.success(f"✅ Data saved for Year {year}")
                                st.rerun()

                            # ========== CALCULATE YEARLY CO₂ REDUCTION ==========
                            if current_input_values and current_output_values and len(current_input_values) == len(
                                    input_ef_list) and len(current_output_values) == len(output_ef_list):

                                def calculate_tracking_emissions(input_values, output_values, amp_value):
                                    """Calculate emissions for tracking based on actual values"""
                                    total_input_emission = 0.0
                                    total_output_emission = 0.0

                                    if is_absolute:
                                        # Absolute calculation
                                        for i, (input_val, input_ef) in enumerate(zip(input_values, input_ef_list)):
                                            total_input_emission += input_val * input_ef

                                        for i, (output_val, output_ef) in enumerate(zip(output_values, output_ef_list)):
                                            total_output_emission += output_val * output_ef
                                    else:
                                        # Specific calculation
                                        for i, (input_val, input_ef) in enumerate(zip(input_values, input_ef_list)):
                                            total_input_emission += (amp_value * input_val) * input_ef

                                        for i, (output_val, output_ef) in enumerate(zip(output_values, output_ef_list)):
                                            # For output in specific mode, values are fixed at 1
                                            total_output_emission += (amp_value * 1.0) * output_ef

                                    net_emission = total_input_emission - total_output_emission
                                    return total_input_emission, total_output_emission, net_emission

                                # Calculate emissions for this year
                                year_input_emission, year_output_emission, year_net_emission = calculate_tracking_emissions(
                                    current_input_values, current_output_values, current_amp_value
                                )

                                # Calculate Sp.Net for this year
                                year_sp_net = 0.0
                                if is_absolute:
                                    current_primary_output = current_output_values[0] if current_output_values else 0.0
                                    year_sp_net = year_net_emission / current_primary_output if current_primary_output != 0 else 0.0
                                else:
                                    year_sp_net = year_net_emission / current_amp_value if current_amp_value != 0 else 0.0

                                # Calculate CO₂ Reduction for this year using same formula as baseline
                                if is_absolute:
                                    year_co2_reduction = (baseline_values.get('sp_net_before',
                                                                              0.0) - year_sp_net) * current_primary_output
                                else:
                                    year_co2_reduction = (baseline_values.get('sp_net_before',
                                                                              0.0) - year_sp_net) * current_amp_value

                                # Store data for summary table
                                year_data_store[year] = {
                                    'year': year,
                                    'input_emission': year_input_emission,
                                    'output_emission': year_output_emission,
                                    'net_emission': year_net_emission,
                                    'sp_net': year_sp_net,
                                    'amp_value': current_amp_value if is_specific else None,
                                    'co2_reduction': year_co2_reduction
                                }

                                # Display CO₂ Reduction for this year
                                st.markdown("---")
                                st.markdown(f"### 📊 Year {year} CO₂ Reduction")

                                col1, col2, col3 = st.columns(3)
                                col1.metric("Baseline CO₂ Reduction",
                                            f"{baseline_values.get('co2_reduction', 0):,.2f} tCO₂e")
                                col2.metric(f"Year {year} CO₂ Reduction", f"{year_co2_reduction:,.2f} tCO₂e",
                                            delta=f"{year_co2_reduction - baseline_values.get('co2_reduction', 0):+,.2f}")

                                # Calculate percentage change
                                if baseline_values.get('co2_reduction', 0) != 0:
                                    pct_change = ((year_co2_reduction - baseline_values.get('co2_reduction', 0)) /
                                                  baseline_values.get('co2_reduction', 0)) * 100
                                    col3.metric("Percentage Change", f"{pct_change:+.1f}%",
                                                delta_color="normal" if pct_change > 0 else "inverse")
                                else:
                                    col3.metric("Percentage Change", "N/A")

                    # ========== SIMPLIFIED SUMMARY TABLE - ONLY CO₂ REDUCTION ==========
                    st.markdown("---")
                    st.markdown("### 📋 CO₂ Reduction Tracking Summary")

                    # Create simple summary table with only CO₂ Reduction
                    summary_headers = ["Period", "CO₂ Reduction (tCO₂e)"]
                    summary_rows = []

                    # Add Baseline row
                    summary_rows.append([
                        "Base",
                        f"{baseline_values.get('co2_reduction', 0):,.2f}"
                    ])

                    # Add rows for years 1 to life_span
                    for year in range(1, life_span + 1):
                        if year in year_data_store:
                            # Year has data - show actual CO₂ Reduction
                            data = year_data_store[year]
                            summary_rows.append([
                                f"{year}-Year",
                                f"{data['co2_reduction']:,.2f}"
                            ])
                        else:
                            # Year has no data yet - show blank/placeholder
                            summary_rows.append([
                                f"{year}-Year",
                                ""  # Blank until data is entered
                            ])

                    # Create DataFrame
                    summary_df = pd.DataFrame(summary_rows, columns=summary_headers)

                    # Display summary table
                    st.dataframe(
                        summary_df,
                        use_container_width=True,
                        hide_index=True,
                        column_config={
                            "Period": st.column_config.TextColumn("Period", width="medium"),
                            "CO₂ Reduction (tCO₂e)": st.column_config.TextColumn("CO₂ Reduction (tCO₂e)", width="large")
                        }
                    )

                    # Calculate totals and averages only for years with data
                    years_with_data = [year for year in range(1, life_span + 1) if year in year_data_store]

                    if years_with_data:
                        # Calculate metrics
                        baseline_reduction = baseline_values.get('co2_reduction', 0)
                        actual_reductions = [year_data_store[year]['co2_reduction'] for year in years_with_data]
                        total_actual_reduction = sum(actual_reductions)
                        avg_actual_reduction = total_actual_reduction / len(
                            actual_reductions) if actual_reductions else 0

                        # Calculate cumulative vs baseline
                        cumulative_baseline = baseline_reduction * len(years_with_data)
                        cumulative_actual = sum(actual_reductions)
                        cumulative_difference = cumulative_actual - cumulative_baseline

                        # Display key metrics
                        col1, col2, col3, col4 = st.columns(4)
                        col1.metric("Years with Data", len(years_with_data))
                        col2.metric("Baseline Reduction", f"{baseline_reduction:,.2f} tCO₂e")
                        col3.metric("Average Actual", f"{avg_actual_reduction:,.2f} tCO₂e")
                        col4.metric("Cumulative Δ", f"{cumulative_difference:+,.2f} tCO₂e",
                                    delta_color="normal" if cumulative_difference > 0 else "inverse")

                        # Visual chart - CO₂ Reduction trend
                        years_for_chart = ["Base"] + [f"Year {y}" for y in years_with_data]
                        reduction_values = [baseline_reduction] + actual_reductions

                        fig = go.Figure()

                        # Add line for baseline
                        fig.add_trace(go.Scatter(
                            x=years_for_chart,
                            y=reduction_values,
                            mode='lines+markers+text',
                            name='CO₂ Reduction',
                            line=dict(color='blue', width=2),
                            marker=dict(size=10),
                            text=[f"{val:,.0f}" for val in reduction_values],
                            textposition='top center'
                        ))

                        # Add horizontal line for baseline
                        fig.add_hline(
                            y=baseline_reduction,
                            line_dash="dash",
                            line_color="gray",
                            annotation_text=f"Baseline: {baseline_reduction:,.0f}",
                            annotation_position="bottom right"
                        )

                        fig.update_layout(
                            title="CO₂ Reduction Trend",
                            xaxis_title="Period",
                            yaxis_title="CO₂ Reduction (tCO₂e)",
                            template="plotly_white",
                            showlegend=True
                        )

                        st.plotly_chart(fig, use_container_width=True)

                        # Export option
                        csv = summary_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download CO₂ Reduction Summary as CSV",
                            data=csv,
                            file_name=f"{project['project_name']}_co2_reduction_summary.csv",
                            mime="text/csv",
                            key="download_co2_reduction_summary"
                        )

                    else:
                        st.info("Enter data for at least one year to see summary metrics and charts.")

                else:
                    st.info("No tracking years available yet. Save the project first.")

                conn.close()

            except Exception as e:
                st.error(f"❌ Tracking error: {str(e)}")
                import traceback
                st.error(f"Traceback: {traceback.format_exc()}")
        else:
            st.info("💡 Save the project first to enable tracking.")
def show_results_popup(emission_results, costing_results, method):
    with st.expander("📊 Calculation Results", expanded=True):
        st.markdown(f"### Results ({method.capitalize() if method else 'Not Selected'} Method)")
        em_data = {
            "Parameter": ["Input CO2", "Output CO2", "Net CO2", "Sp.Net", "CO2 reduction"],
            "UOM": ["kg", "kg", "kg", "kg/tp", "kg"],
            "Before": [emission_results["Input CO2_Before"], emission_results["Output CO2_Before"], emission_results["Net CO2_Before"], emission_results["Sp.Net_Before"], ""],
            "After": [emission_results["Input CO2_After"], emission_results["Output CO2_After"], emission_results["Net CO2_After"], emission_results["Sp.Net_After"], ""],
            "Net": [emission_results["Input CO2_Net"], emission_results["Output CO2_Net"], emission_results["Net CO2_Net"], emission_results["Sp.Net_Net"], emission_results["CO2 reduction_Net"]]
        }
        st.dataframe(pd.DataFrame(em_data), use_container_width=True, hide_index=True)

        cost_data = {
            "Parameter": ["OPEX Cost (Excluding Fuel and Energy)", "OPEX Cost (Only Fuel and Energy)"],
            "UOM": ["INR", "INR"],
            "Before": [costing_results["OPEX Cost (Excluding Fuel and Energy)_Before"], costing_results["OPEX Cost (Only Fuel and Energy)_Before"]],
            "After": [costing_results["OPEX Cost (Excluding Fuel and Energy)_After"], costing_results["OPEX Cost (Only Fuel and Energy)_After"]],
            "Net": [costing_results["OPEX Cost (Excluding Fuel and Energy)_Net"], costing_results["OPEX Cost (Only Fuel and Energy)_Net"]]
        }
        st.dataframe(pd.DataFrame(cost_data), use_container_width=True, hide_index=True)
def load_project_data(code):
    conn = sqlite3.connect(PROJECT_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM projects WHERE project_code = ?", (code,))
    row = cursor.fetchone()
    if not row:
        conn.close()
        st.error("Project not found")
        return

    cursor.execute("PRAGMA table_info(projects)")
    columns = [info[1] for info in cursor.fetchall()]
    data = dict(zip(columns, row))
    conn.close()

    state = st.session_state.project_state
    state['current_project_code'] = code
    state['calculation_method'] = data.get('calculation_method')

    mapping = {
        "Organization:": "organization",
        "Entity Name:": "entity_name",
        "Unit Name:": "unit_name",
        "Project Name:": "project_name",
        "Base Year:": "base_year",
        "Target Year:": "target_year",
        "Implementation Date:": "implementation_date",
        "CAPEX (INR):": "capex",
        "Life Span (Years):": "life_span",
        "Project Owner:": "project_owner"
    }
    for label, field in mapping.items():
        state['general_info'][label] = data.get(field) or ""

    state['input_rows'] = json.loads(data.get('input_data') or "[]")
    state['output_rows'] = json.loads(data.get('output_data') or '[["Primary Output","","","","1","1",""]]')
    state['costing_rows'] = json.loads(data.get('costing_data') or '[["OPEX Cost (Excluding Fuel and Energy)","INR","","","",""],["OPEX Cost (Only Fuel and Energy)","INR","","","",""]]')
    state['amp_uom'] = data.get('amp_uom') or "kg"
    state['amp_before'] = str(data.get('amp_before') or "")
    state['amp_after'] = str(data.get('amp_after') or "")

    st.rerun()
def show_tracking_dialog(project_code):
    st.subheader(f"📈 Track Actuals – {project_code}")
    conn = sqlite3.connect(PROJECT_DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT input_data, output_data FROM projects WHERE project_code = ?", (project_code,))
    proj = cursor.fetchone()
    if not proj:
        st.error("Project data not found")
        return
    input_data = json.loads(proj[0])
    output_data = json.loads(proj[1])

    years = sorted(set([r[0] for r in cursor.execute("SELECT DISTINCT year_number FROM project_actuals WHERE project_code = ?", (project_code,)).fetchall()] +
                      [r[0] for r in cursor.execute("SELECT DISTINCT year_number FROM amp_actuals_tracking WHERE project_code = ?", (project_code,)).fetchall()]))

    year = st.selectbox("Year", ["New Year"] + years, key="track_year_select")
    if year == "New Year":
        year = st.number_input("Enter Year", 2000, 2100, step=1, key="new_track_year")
        if not st.button("Create Year", key="create_year_btn"):
            st.stop()

    for idx, row in enumerate(input_data):
        material = row[0] or f"Input {idx+1}"
        st.markdown(f"**{material}**")
        c1, c2 = st.columns(2)
        abs_v = c1.number_input("Absolute Value", min_value=0.0, key=f"track_in_abs_{idx}_{year}")
        spec_v = c2.number_input("Specific Value", min_value=0.0, key=f"track_in_spec_{idx}_{year}")
        if st.button("Save", key=f"track_save_in_{idx}_{year}"):
            cursor.execute('''
                INSERT OR REPLACE INTO project_actuals 
                (project_code, section_type, material_name, row_index, year_number, absolute_value, specific_value)
                VALUES (?, 'input', ?, ?, ?, ?, ?)
            ''', (project_code, material, idx, year, abs_v or None, spec_v or None))
            conn.commit()
            st.success("Saved")

    for idx, row in enumerate(output_data):
        material = row[0] or f"Output {idx+1}"
        st.markdown(f"**{material}**")
        c1, c2 = st.columns(2)
        abs_v = c1.number_input("Absolute Value", min_value=0.0, key=f"track_out_abs_{idx}_{year}")
        spec_v = c2.number_input("Specific Value", min_value=0.0, key=f"track_out_spec_{idx}_{year}")
        if st.button("Save", key=f"track_save_out_{idx}_{year}"):
            cursor.execute('''
                INSERT OR REPLACE INTO project_actuals 
                (project_code, section_type, material_name, row_index, year_number, absolute_value, specific_value)
                VALUES (?, 'output', ?, ?, ?, ?, ?)
            ''', (project_code, material, idx, year, abs_v or None, spec_v or None))
            conn.commit()
            st.success("Saved")

    st.markdown("**AMP Actual**")
    amp_v = st.number_input("AMP Value", min_value=0.0, key=f"track_amp_{year}")
    if st.button("Save AMP", key="save_amp_btn"):
        cursor.execute("INSERT OR REPLACE INTO amp_actuals_tracking (project_code, year_number, amp_value) VALUES (?, ?, ?)",
                       (project_code, year, amp_v or None))
        conn.commit()
        st.success("Saved")

    conn.close()
# --- MAIN ---
def main():
    st.sidebar.title("🌿 Decarbonization Suite")
    page = st.sidebar.radio("Select Module",
                            ["Fuel & Energy Calculator",
                             "CO2 Project Calculator",   # <--- NEW MODULE PLACED HERE
                             "MACC Calculator",
                             "Strategy Dashboard"])

    if page == "Fuel & Energy Calculator":
        fuel_energy_calculator_ui()
    elif page == "CO2 Project Calculator":
        co2_project_calculator_ui()
    elif page == "MACC Calculator":
        npv_project_analysis_ui()
    elif page == "Strategy Dashboard":
        strategy_dashboard_ui()
if __name__ == "__main__":
    main()