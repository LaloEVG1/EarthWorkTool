import math
import sqlite3
from datetime import datetime

import pandas as pd
import plotly.express as px
import folium
from streamlit_folium import st_folium
import streamlit as st


# =========================
# CONFIG
# =========================
st.set_page_config(
    page_title="Earth Material Management",
    page_icon="🛠️",
    layout="wide"
)

DB_PATH = "earth_materials.db"


# =========================
# DATABASE
# =========================
def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS banks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        quality TEXT NOT NULL,
        available_volume REAL NOT NULL,
        reserved_volume REAL NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'Disponible',
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS projects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        latitude REAL NOT NULL,
        longitude REAL NOT NULL,
        required_quality TEXT NOT NULL,
        required_volume REAL NOT NULL,
        received_volume REAL NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'Activo',
        updated_at TEXT NOT NULL
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        bank_id INTEGER NOT NULL,
        project_id INTEGER NOT NULL,
        volume REAL NOT NULL,
        quality TEXT NOT NULL,
        distance_km REAL,
        status TEXT NOT NULL DEFAULT 'Pendiente',
        requested_at TEXT NOT NULL,
        approved_at TEXT,
        completed_at TEXT,
        notes TEXT,
        FOREIGN KEY(bank_id) REFERENCES banks(id),
        FOREIGN KEY(project_id) REFERENCES projects(id)
    )
    """)

    conn.commit()
    conn.close()


def update_bank_volume(bank_id, new_volume):
    conn = get_connection()
    cur = conn.cursor()

    # Prevent invalid state
    cur.execute("SELECT reserved_volume FROM banks WHERE id = ?", (bank_id,))
    reserved = cur.fetchone()[0]

    if new_volume < reserved:
        conn.close()
        return False, f"Volume cannot be less than reserved ({reserved})"

    cur.execute("""
        UPDATE banks
        SET available_volume = ?, updated_at = ?
        WHERE id = ?
    """, (new_volume, now_str(), bank_id))

    conn.commit()
    conn.close()
    return True, "Bank volume updated"


def delete_bank(bank_id):
    conn = get_connection()
    cur = conn.cursor()

    # Check if there are active transactions
    cur.execute("""
        SELECT COUNT(*) FROM transactions
        WHERE bank_id = ? AND status IN ('Pendiente', 'Aprobada')
    """, (bank_id,))
    count = cur.fetchone()[0]

    if count > 0:
        conn.close()
        return False, "Cannot delete bank with active transactions"

    cur.execute("DELETE FROM banks WHERE id = ?", (bank_id,))
    conn.commit()
    conn.close()
    return True, "Bank deleted"


def update_project_required_volume(project_id, new_volume):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT received_volume
        FROM projects
        WHERE id = ?
    """, (project_id,))
    row = cur.fetchone()

    if row is None:
        conn.close()
        return False, "Project not found."

    received_volume = row[0]

    if new_volume < received_volume:
        conn.close()
        return False, f"Required volume cannot be less than received volume ({received_volume:.2f})"

    cur.execute("""
        UPDATE projects
        SET required_volume = ?, updated_at = ?
        WHERE id = ?
    """, (new_volume, now_str(), project_id))

    conn.commit()
    conn.close()
    return True, "Project required volume updated."


def delete_project(project_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT COUNT(*)
        FROM transactions
        WHERE project_id = ?
          AND status IN ('Pendiente', 'Aprobada')
    """, (project_id,))
    count = cur.fetchone()[0]

    if count > 0:
        conn.close()
        return False, "Cannot delete project with active transactions."

    cur.execute("DELETE FROM projects WHERE id = ?", (project_id,))
    conn.commit()
    conn.close()
    return True, "Project deleted."




def seed_data():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM banks")
    banks_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM projects")
    projects_count = cur.fetchone()[0]

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if banks_count == 0:
        sample_banks = [
            ("TEST", 25.812, -100.313, "Terraplen", 0, 0, "Disponible", now),
        ]
        cur.executemany("""
            INSERT INTO banks (
                name, latitude, longitude, quality, available_volume,
                reserved_volume, status, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, sample_banks)

    if projects_count == 0:
        sample_projects = [
            ("TEST", 25.801, -100.402, "Terraplen", 0, 0, "Activo", now),
        ]
        cur.executemany("""
            INSERT INTO projects (
                name, latitude, longitude, required_quality, required_volume,
                received_volume, status, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, sample_projects)

    conn.commit()
    conn.close()


def load_banks():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM banks ORDER BY id DESC", conn)
    conn.close()
    return df


def load_projects():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM projects ORDER BY id DESC", conn)
    conn.close()
    return df


def load_transactions():
    conn = get_connection()
    query = """
        SELECT
            t.id,
            t.bank_id,
            b.name AS bank_name,
            t.project_id,
            p.name AS project_name,
            t.volume,
            t.quality,
            t.distance_km,
            t.status,
            t.requested_at,
            t.approved_at,
            t.completed_at,
            t.notes
        FROM transactions t
        LEFT JOIN banks b ON t.bank_id = b.id
        LEFT JOIN projects p ON t.project_id = p.id
        ORDER BY t.id DESC
    """
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


# =========================
# HELPERS
# =========================
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def add_bank(name, lat, lon, quality, volume, status):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO banks (
            name, latitude, longitude, quality, available_volume,
            reserved_volume, status, updated_at
        )
        VALUES (?, ?, ?, ?, ?, 0, ?, ?)
    """, (name, lat, lon, quality, volume, status, now_str()))
    conn.commit()
    conn.close()


def add_project(name, lat, lon, quality, volume, status):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO projects (
            name, latitude, longitude, required_quality, required_volume,
            received_volume, status, updated_at
        )
        VALUES (?, ?, ?, ?, ?, 0, ?, ?)
    """, (name, lat, lon, quality, volume, status, now_str()))
    conn.commit()
    conn.close()


def create_transaction(bank_id, project_id, volume, notes=""):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM banks WHERE id = ?", (bank_id,))
    bank = cur.fetchone()

    cur.execute("SELECT * FROM projects WHERE id = ?", (project_id,))
    project = cur.fetchone()

    if bank is None or project is None:
        conn.close()
        return False, "Bank or project not found."

    bank_quality = bank[4]
    available_volume = bank[5]
    reserved_volume = bank[6]
    bank_lat = bank[2]
    bank_lon = bank[3]

    project_quality = project[4]
    required_volume = project[5]
    received_volume = project[6]
    project_lat = project[2]
    project_lon = project[3]

    free_volume = available_volume - reserved_volume
    missing_volume = required_volume - received_volume

    if bank_quality != project_quality:
        conn.close()
        return False, "Quality mismatch between bank and project."

    if volume > free_volume:
        conn.close()
        return False, f"Not enough free material. Free volume: {free_volume:.2f}"

    if volume > missing_volume:
        conn.close()
        return False, f"Requested volume exceeds project need. Missing volume: {missing_volume:.2f}"

    distance_km = haversine_km(bank_lat, bank_lon, project_lat, project_lon)

    cur.execute("""
        INSERT INTO transactions (
            bank_id, project_id, volume, quality, distance_km,
            status, requested_at, notes
        )
        VALUES (?, ?, ?, ?, ?, 'Pendiente', ?, ?)
    """, (bank_id, project_id, volume, bank_quality, distance_km, now_str(), notes))

    cur.execute("""
        UPDATE banks
        SET reserved_volume = reserved_volume + ?, updated_at = ?
        WHERE id = ?
    """, (volume, now_str(), bank_id))

    conn.commit()
    conn.close()
    return True, "Transaction created successfully."


def approve_transaction(transaction_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT id, status FROM transactions WHERE id = ?", (transaction_id,))
    trx = cur.fetchone()
    if trx is None:
        conn.close()
        return False, "Transaction not found."

    if trx[1] != "Pendiente":
        conn.close()
        return False, "Only pending transactions can be approved."

    cur.execute("""
        UPDATE transactions
        SET status = 'Aprobada', approved_at = ?
        WHERE id = ?
    """, (now_str(), transaction_id))

    conn.commit()
    conn.close()
    return True, "Transaction approved."


def reject_transaction(transaction_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT bank_id, volume, status
        FROM transactions
        WHERE id = ?
    """, (transaction_id,))
    trx = cur.fetchone()

    if trx is None:
        conn.close()
        return False, "Transaction not found."

    bank_id, volume, status = trx

    if status != "Pendiente":
        conn.close()
        return False, "Only pending transactions can be rejected."

    cur.execute("""
        UPDATE banks
        SET reserved_volume = reserved_volume - ?, updated_at = ?
        WHERE id = ?
    """, (volume, now_str(), bank_id))

    cur.execute("""
        UPDATE transactions
        SET status = 'Rechazada'
        WHERE id = ?
    """, (transaction_id,))

    conn.commit()
    conn.close()
    return True, "Transaction rejected."


def complete_transaction(transaction_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT bank_id, project_id, volume, status
        FROM transactions
        WHERE id = ?
    """, (transaction_id,))
    trx = cur.fetchone()

    if trx is None:
        conn.close()
        return False, "Transaction not found."

    bank_id, project_id, volume, status = trx

    if status != "Aprobada":
        conn.close()
        return False, "Only approved transactions can be completed."

    cur.execute("""
        UPDATE banks
        SET available_volume = available_volume - ?,
            reserved_volume = reserved_volume - ?,
            updated_at = ?
        WHERE id = ?
    """, (volume, volume, now_str(), bank_id))

    cur.execute("""
        UPDATE projects
        SET received_volume = received_volume + ?,
            updated_at = ?
        WHERE id = ?
    """, (volume, now_str(), project_id))

    cur.execute("""
        UPDATE transactions
        SET status = 'Completada',
            completed_at = ?
        WHERE id = ?
    """, (now_str(), transaction_id))

    conn.commit()
    conn.close()
    return True, "Transaction completed."


def get_recommendations(project_id):
    banks = load_banks()
    projects = load_projects()

    if banks.empty or projects.empty:
        return pd.DataFrame()

    project_row = projects.loc[projects["id"] == project_id]
    if project_row.empty:
        return pd.DataFrame()

    project = project_row.iloc[0]
    required_quality = project["required_quality"]
    missing_volume = project["required_volume"] - project["received_volume"]

    if missing_volume <= 0:
        return pd.DataFrame()

    compatible_banks = banks[
        (banks["quality"] == required_quality) &
        (banks["status"] == "Disponible")
    ].copy()

    if compatible_banks.empty:
        return pd.DataFrame()

    compatible_banks["free_volume"] = (
        compatible_banks["available_volume"] - compatible_banks["reserved_volume"]
    )

    compatible_banks = compatible_banks[compatible_banks["free_volume"] > 0].copy()

    if compatible_banks.empty:
        return pd.DataFrame()

    compatible_banks["distance_km"] = compatible_banks.apply(
        lambda row: haversine_km(
            row["latitude"], row["longitude"],
            project["latitude"], project["longitude"]
        ),
        axis=1
    )

    compatible_banks["recommended_volume"] = compatible_banks["free_volume"].apply(
        lambda x: min(x, missing_volume)
    )

    compatible_banks["score"] = (
        compatible_banks["distance_km"] * 0.6
        - compatible_banks["recommended_volume"] / 1000 * 0.4
    )

    compatible_banks = compatible_banks.sort_values(by=["score", "distance_km"])

    return compatible_banks[[
        "id", "name", "quality", "free_volume",
        "distance_km", "recommended_volume", "status"
    ]].reset_index(drop=True)


def to_csv_download(df):
    return df.to_csv(index=False).encode("utf-8")


# =========================
# INIT
# =========================
init_db()
seed_data()

st.title("🛠️ Earth Material Management")
st.caption("Dashboard for available material, projects, map, and transaction control.")

# =========================
# LOAD DATA
# =========================
banks_df = load_banks()
projects_df = load_projects()
transactions_df = load_transactions()

if not banks_df.empty:
    banks_df["free_volume"] = banks_df["available_volume"] - banks_df["reserved_volume"]
else:
    banks_df["free_volume"] = []

if not projects_df.empty:
    projects_df["missing_volume"] = projects_df["required_volume"] - projects_df["received_volume"]
else:
    projects_df["missing_volume"] = []


# =========================
# SIDEBAR
# =========================
st.sidebar.header("Navigation")
section = st.sidebar.radio(
    "Go to",
    [
        "Dashboard",
        "Map",
        "Add Bank",
        "Add Project",
        "Recommendations",
        "Transactions",
        "Data Tables"
    ]
)


# =========================
# DASHBOARD
# =========================
if section == "Dashboard":
    st.subheader("General Dashboard")

    total_available = float(banks_df["available_volume"].sum()) if not banks_df.empty else 0.0
    total_reserved = float(banks_df["reserved_volume"].sum()) if not banks_df.empty else 0.0
    total_free = float(banks_df["free_volume"].sum()) if not banks_df.empty else 0.0
    total_needed = float(projects_df["missing_volume"].clip(lower=0).sum()) if not projects_df.empty else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Available Volume", f"{total_available:,.2f} m³")
    c2.metric("Reserved Volume", f"{total_reserved:,.2f} m³")
    c3.metric("Free Volume", f"{total_free:,.2f} m³")
    c4.metric("Pending Need", f"{total_needed:,.2f} m³")

    left, right = st.columns(2)

    with left:
        st.markdown("### Volume by Quality")
        if not banks_df.empty:
            chart_df = banks_df.groupby("quality", as_index=False)["available_volume"].sum()
            fig = px.pie(
                chart_df,
                names="quality",
                values="available_volume",
                title="Available Material by Quality"
            )
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No banks available yet.")

    with right:
        st.markdown("### Recent Transactions")
        if not transactions_df.empty:
            st.dataframe(
                transactions_df[[
                    "id", "bank_name", "project_name", "volume",
                    "quality", "status", "requested_at"
                ]].head(10),
                use_container_width=True
            )
        else:
            st.info("No transactions yet.")

    st.markdown("### Material by Bank")
    if not banks_df.empty:
        bank_chart = banks_df[["name", "available_volume", "reserved_volume", "free_volume"]].copy()
        st.dataframe(bank_chart, use_container_width=True)
    else:
        st.info("No banks available.")


# =========================
# MAP
# =========================
elif section == "Map":
    st.subheader("Interactive Map (Folium)")

    # Create base map centered in Monterrey
    m = folium.Map(location=[25.70, -100.30], zoom_start=10)

    # Add banks (blue)
    if not banks_df.empty:
        for _, row in banks_df.iterrows():
            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=7,
                popup=f"""
                    <b>{row['name']}</b><br>
                    Type: Bank<br>
                    Quality: {row['quality']}<br>
                    Available: {row['available_volume']} m³
                """,
                color="blue",
                fill=True,
                fill_opacity=0.8
            ).add_to(m)

    # Add projects (red)
    if not projects_df.empty:
        for _, row in projects_df.iterrows():
            missing = row["required_volume"] - row["received_volume"]
            folium.CircleMarker(
                location=[row["latitude"], row["longitude"]],
                radius=7,
                popup=f"""
                    <b>{row['name']}</b><br>
                    Type: Project<br>
                    Quality: {row['required_quality']}<br>
                    Missing: {missing} m³
                """,
                color="red",
                fill=True,
                fill_opacity=0.8
            ).add_to(m)

    # Display map
    st_folium(m, width=None, height=600)


# =========================
# ADD BANK
# =========================
elif section == "Add Bank":
    st.subheader("Add New Bank")

    with st.form("add_bank_form"):
        name = st.text_input("Bank name")
        col1, col2 = st.columns(2)
        with col1:
            latitude = st.number_input("Latitude", value=25.7000, format="%.6f")
        with col2:
            longitude = st.number_input("Longitude", value=-100.3000, format="%.6f")

        quality = st.selectbox("Quality", ["Terraplen", "Subrasante"])
        volume = st.number_input("Available volume (m³)", min_value=0.0, value=1000.0, step=100.0)
        status = st.selectbox("Status", ["Disponible", "Reservado", "Agotado"])

        submitted = st.form_submit_button("Add bank")

        if submitted:
            if not name.strip():
                st.error("Please enter a bank name.")
            else:
                add_bank(name.strip(), latitude, longitude, quality, volume, status)
                st.success("Bank added successfully.")
                st.rerun()


# =========================
# ADD PROJECT
# =========================
elif section == "Add Project":
    st.subheader("Add New Project")

    with st.form("add_project_form"):
        name = st.text_input("Project name")
        col1, col2 = st.columns(2)
        with col1:
            latitude = st.number_input("Project latitude", value=25.7000, format="%.6f")
        with col2:
            longitude = st.number_input("Project longitude", value=-100.3000, format="%.6f")

        quality = st.selectbox("Required quality", ["Terraplen", "Subrasante"])
        volume = st.number_input("Required volume (m³)", min_value=0.0, value=1000.0, step=100.0)
        status = st.selectbox("Project status", ["Activo", "Pausado", "Completado"])

        submitted = st.form_submit_button("Add project")

        if submitted:
            if not name.strip():
                st.error("Please enter a project name.")
            else:
                add_project(name.strip(), latitude, longitude, quality, volume, status)
                st.success("Project added successfully.")
                st.rerun()


# =========================
# RECOMMENDATIONS
# =========================
elif section == "Recommendations":
    st.subheader("Best Source Recommendation")

    if projects_df.empty:
        st.info("No projects available.")
    else:
        project_options = {
            f"{row['name']} | Need: {max(row['missing_volume'], 0):,.2f} m³ | {row['required_quality']}": row["id"]
            for _, row in projects_df.iterrows()
            if row["status"] == "Activo"
        }

        if not project_options:
            st.info("No active projects available.")
        else:
            selected_label = st.selectbox("Select project", list(project_options.keys()))
            project_id = project_options[selected_label]

            recs = get_recommendations(project_id)

            if recs.empty:
                st.warning("No compatible banks found for this project.")
            else:
                st.dataframe(recs, use_container_width=True)

                st.markdown("### Create transaction from recommendation")
                bank_ids = recs["id"].tolist()

                with st.form("create_recommended_transaction"):
                    bank_id = st.selectbox("Recommended bank", bank_ids)
                    max_vol = float(recs.loc[recs["id"] == bank_id, "recommended_volume"].iloc[0])
                    volume = st.number_input(
                        "Volume to request (m³)",
                        min_value=0.0,
                        max_value=max_vol,
                        value=min(max_vol, 1000.0),
                        step=100.0
                    )
                    notes = st.text_area("Notes", "")
                    submitted = st.form_submit_button("Create transaction")

                    if submitted:
                        ok, msg = create_transaction(bank_id, project_id, volume, notes)
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)


# =========================
# TRANSACTIONS
# =========================
elif section == "Transactions":
    st.subheader("Transaction Management")

    tab1, tab2 = st.tabs(["Create Transaction", "Manage Existing"])

    with tab1:
        if banks_df.empty or projects_df.empty:
            st.info("You need at least one bank and one project.")
        else:
            compatible_projects = projects_df[projects_df["status"] == "Activo"].copy()
            compatible_banks = banks_df[banks_df["status"] == "Disponible"].copy()

            if compatible_projects.empty or compatible_banks.empty:
                st.info("No active projects or available banks.")
            else:
                with st.form("manual_transaction_form"):
                    bank_label_map = {
                        f"{row['name']} | {row['quality']} | Free: {row['free_volume']:,.2f} m³": row["id"]
                        for _, row in compatible_banks.iterrows()
                    }
                    project_label_map = {
                        f"{row['name']} | {row['required_quality']} | Missing: {max(row['missing_volume'], 0):,.2f} m³": row["id"]
                        for _, row in compatible_projects.iterrows()
                    }

                    bank_label = st.selectbox("Select bank", list(bank_label_map.keys()))
                    project_label = st.selectbox("Select project", list(project_label_map.keys()))
                    volume = st.number_input("Volume (m³)", min_value=0.0, value=1000.0, step=100.0)
                    notes = st.text_area("Notes", "")

                    submitted = st.form_submit_button("Create transaction")

                    if submitted:
                        bank_id = bank_label_map[bank_label]
                        project_id = project_label_map[project_label]
                        ok, msg = create_transaction(bank_id, project_id, volume, notes)
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)

    with tab2:
        transactions_df = load_transactions()

        if transactions_df.empty:
            st.info("No transactions found.")
        else:
            st.dataframe(transactions_df, use_container_width=True)

            trx_ids = transactions_df["id"].tolist()
            selected_id = st.selectbox("Select transaction ID", trx_ids)

            selected_status = transactions_df.loc[
                transactions_df["id"] == selected_id, "status"
            ].iloc[0]

            c1, c2, c3 = st.columns(3)

            with c1:
                if st.button("Approve", use_container_width=True):
                    ok, msg = approve_transaction(selected_id)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

            with c2:
                if st.button("Reject", use_container_width=True):
                    ok, msg = reject_transaction(selected_id)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

            with c3:
                if st.button("Complete", use_container_width=True):
                    ok, msg = complete_transaction(selected_id)
                    if ok:
                        st.success(msg)
                        st.rerun()
                    else:
                        st.error(msg)

            st.info(f"Current status: {selected_status}")


# =========================
# DATA TABLES
# =========================
elif section == "Data Tables":
    st.subheader("Data Tables and Export")

    tab1, tab2, tab3 = st.tabs(["Banks", "Projects", "Transactions"])



    with tab1:
        st.markdown("### Manage Banks")
    
        if banks_df.empty:
            st.info("No banks available.")
        else:
            for _, row in banks_df.iterrows():
                with st.container():
                    col1, col2, col3, col4 = st.columns([3, 2, 2, 2])
        
                    with col1:
                        st.markdown(f"**{row['name']}**")
                        st.caption(f"{row['quality']} | Available: {row['available_volume']:.0f} | Reserved: {row['reserved_volume']:.0f}")
        
                    with col2:
                        new_volume = st.number_input(
                            f"Edit volume (Bank {row['id']})",
                            min_value=0.0,
                            value=float(row["available_volume"]),
                            key=f"vol_{row['id']}"
                        )
        
                    with col3:
                        if st.button("Update", key=f"update_{row['id']}"):
                            ok, msg = update_bank_volume(row["id"], new_volume)
                            if ok:
                                st.success(msg)
                                st.rerun()
                            else:
                                st.error(msg)
        
                    with col4:
                        if st.button("Delete", key=f"delete_{row['id']}"):
                            ok, msg = delete_bank(row["id"])
                            if ok:
                                st.success(msg)
                                st.rerun()
                            else:
                                st.error(msg)
        
                    st.divider()
        st.download_button(
            "Download Banks CSV",
            data=to_csv_download(banks_df),
            file_name="banks.csv",
            mime="text/csv"
        )
    
    #with tab1:
        #st.dataframe(banks_df, use_container_width=True)
        #st.download_button(
            #"Download Banks CSV",
            #data=to_csv_download(banks_df),
            #file_name="banks.csv",
            #mime="text/csv"
        #)

with tab2:
    st.markdown("### Manage Projects")

    if projects_df.empty:
        st.info("No projects available.")
    else:
        for _, row in projects_df.iterrows():
            with st.container():
                col1, col2, col3, col4 = st.columns([3, 2, 2, 2])

                with col1:
                    missing = row["required_volume"] - row["received_volume"]
                    st.markdown(f"**{row['name']}**")
                    st.caption(
                        f"{row['required_quality']} | Required: {row['required_volume']:.0f} | "
                        f"Received: {row['received_volume']:.0f} | Missing: {missing:.0f}"
                    )

                with col2:
                    new_required_volume = st.number_input(
                        f"Edit required volume (Project {row['id']})",
                        min_value=0.0,
                        value=float(row["required_volume"]),
                        key=f"proj_vol_{row['id']}"
                    )

                with col3:
                    if st.button("Update", key=f"update_project_{row['id']}"):
                        ok, msg = update_project_required_volume(row["id"], new_required_volume)
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)

                with col4:
                    if st.button("Delete", key=f"delete_project_{row['id']}"):
                        ok, msg = delete_project(row["id"])
                        if ok:
                            st.success(msg)
                            st.rerun()
                        else:
                            st.error(msg)

                st.divider()

    st.download_button(
        "Download Projects CSV",
        data=to_csv_download(projects_df),
        file_name="projects.csv",
        mime="text/csv"
    )

    with tab3:
        st.dataframe(transactions_df, use_container_width=True)
        st.download_button(
            "Download Transactions CSV",
            data=to_csv_download(transactions_df),
            file_name="transactions.csv",
            mime="text/csv"
        )
