import math
import psycopg2
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
    page_title="ADMINISTRACIÓN DE MOVIMIENTO DE TIERRAS",
    page_icon="🛠️",
    layout="wide"
)




# =========================
# DATABASE
# =========================
def get_connection():
    return psycopg2.connect(st.secrets["DATABASE_URL"])


def init_db():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS banks (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        macroproyecto TEXT NOT NULL DEFAULT 'SIN ASIGNAR',
        latitude DOUBLE PRECISION NOT NULL,
        longitude DOUBLE PRECISION NOT NULL,
        quality TEXT NOT NULL,
        available_volume DOUBLE PRECISION NOT NULL,
        reserved_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'Disponible',
        updated_at TIMESTAMP NOT NULL
    )
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS projects (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL,
        latitude DOUBLE PRECISION NOT NULL,
        longitude DOUBLE PRECISION NOT NULL,
        required_quality TEXT NOT NULL,
        required_volume DOUBLE PRECISION NOT NULL,
        received_volume DOUBLE PRECISION NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT 'Activo',
        updated_at TIMESTAMP NOT NULL
    )
    """)
    
    cur.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        id SERIAL PRIMARY KEY,
        bank_id INTEGER NOT NULL REFERENCES banks(id),
        project_id INTEGER NOT NULL REFERENCES projects(id),
        volume DOUBLE PRECISION NOT NULL,
        quality TEXT NOT NULL,
        distance_km DOUBLE PRECISION,
        status TEXT NOT NULL DEFAULT 'Pendiente',
        requested_at TIMESTAMP NOT NULL,
        approved_at TIMESTAMP,
        completed_at TIMESTAMP,
        notes TEXT
    )
    """)

    conn.commit()
    conn.close()


def update_bank_volume(bank_id, new_volume):
    conn = get_connection()
    cur = conn.cursor()

    # Prevent invalid state
    cur.execute("SELECT reserved_volume FROM banks WHERE id = %s", (bank_id,))
    reserved = cur.fetchone()[0]

    if new_volume < reserved:
        conn.close()
        return False, f"Volume cannot be less than reserved ({reserved})"

    cur.execute("""
        UPDATE banks
        SET available_volume = %s, updated_at = %s
        WHERE id = %s
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
        WHERE bank_id = %s AND status IN ('Pendiente', 'Aprobada')
    """, (bank_id,))
    count = cur.fetchone()[0]

    if count > 0:
        conn.close()
        return False, "Cannot delete bank with active transactions"

    cur.execute("DELETE FROM banks WHERE id = %s", (bank_id,))
    conn.commit()
    conn.close()
    return True, "Bank deleted"


def update_project_required_volume(project_id, new_volume):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT received_volume
        FROM projects
        WHERE id = %s
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
        SET required_volume = %s, updated_at = %s
        WHERE id = %s
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
        WHERE project_id = %s
          AND status IN ('Pendiente', 'Aprobada')
    """, (project_id,))
    count = cur.fetchone()[0]

    if count > 0:
        conn.close()
        return False, "Cannot delete project with active transactions."

    cur.execute("DELETE FROM projects WHERE id = %s", (project_id,))
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
            ("TEST", "DOMINIO CUMBRES",25.812, -100.313, "Terraplen", 0, 0, "Disponible", now),
        ]
        cur.executemany("""
            INSERT INTO banks (
                name, macroproyecto, latitude, longitude, quality, available_volume,
                reserved_volume, status, updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, sample_projects)

    conn.commit()
    conn.close()


@st.cache_data(ttl=10)
def load_banks():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM banks ORDER BY id DESC", conn)
    conn.close()
    return df


@st.cache_data(ttl=10)
def load_projects():
    conn = get_connection()
    df = pd.read_sql_query("SELECT * FROM projects ORDER BY id DESC", conn)
    conn.close()
    return df


@st.cache_data(ttl=10)
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


def update_bank_macroproyecto(bank_id, new_macroproyecto):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        UPDATE banks
        SET macroproyecto = %s, updated_at = %s
        WHERE id = %s
    """, (new_macroproyecto, now_str(), bank_id))

    conn.commit()
    conn.close()
    return True, "Macroproyecto updated."




def add_bank(name, macroproyecto,lat, lon, quality, volume, status):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO banks (
            name, macroproyecto,latitude, longitude, quality, available_volume,
            reserved_volume, status, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, 0, %s, %s)
    """, (name, macroproyecto, lat, lon, quality, volume, status, now_str()))
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
        VALUES (%s, %s, %s, %s, %s, 0, %s, %s)
    """, (name, lat, lon, quality, volume, status, now_str()))
    conn.commit()
    conn.close()


def create_transaction(bank_id, project_id, volume, notes=""):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT * FROM banks WHERE id = %s", (bank_id,))
    bank = cur.fetchone()

    cur.execute("SELECT * FROM projects WHERE id = %s", (project_id,))
    project = cur.fetchone()

    if bank is None or project is None:
        conn.close()
        return False, "Bank or project not found."

    # banks table with macroproyecto:
    # 0 id
    # 1 name
    # 2 macroproyecto
    # 3 latitude
    # 4 longitude
    # 5 quality
    # 6 available_volume
    # 7 reserved_volume
    # 8 status
    # 9 updated_at

    bank_quality = bank[5]
    available_volume = float(bank[6])
    reserved_volume = float(bank[7])
    bank_lat = float(bank[3])
    bank_lon = float(bank[4])

    # projects table:
    # 0 id
    # 1 name
    # 2 latitude
    # 3 longitude
    # 4 required_quality
    # 5 required_volume
    # 6 received_volume
    # 7 status
    # 8 updated_at

    project_quality = project[4]
    required_volume = float(project[5])
    received_volume = float(project[6])
    project_lat = float(project[2])
    project_lon = float(project[3])

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
        VALUES (%s, %s, %s, %s, %s, 'Pendiente', %s, %s)
    """, (bank_id, project_id, volume, bank_quality, distance_km, now_str(), notes))

    cur.execute("""
        UPDATE banks
        SET reserved_volume = reserved_volume + %s, updated_at = %s
        WHERE id = %s
    """, (volume, now_str(), bank_id))

    conn.commit()
    conn.close()
    return True, "Transaction created successfully."


def approve_transaction(transaction_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("SELECT id, status FROM transactions WHERE id = %s", (transaction_id,))
    trx = cur.fetchone()
    if trx is None:
        conn.close()
        return False, "Transaction not found."

    if trx[1] != "Pendiente":
        conn.close()
        return False, "Only pending transactions can be approved."

    cur.execute("""
        UPDATE transactions
        SET status = 'Aprobada', approved_at = %s
        WHERE id = %s
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
        WHERE id = %s
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
        SET reserved_volume = reserved_volume - %s, updated_at = %s
        WHERE id = %s
    """, (volume, now_str(), bank_id))

    cur.execute("""
        UPDATE transactions
        SET status = 'Rechazada'
        WHERE id = %s
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
        WHERE id = %s
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
        SET available_volume = available_volume - %s,
            reserved_volume = reserved_volume - %s,
            updated_at = %s
        WHERE id = %s
    """, (volume, volume, now_str(), bank_id))

    cur.execute("""
        UPDATE projects
        SET received_volume = received_volume + %s,
            updated_at = %s
        WHERE id = %s
    """, (volume, now_str(), project_id))

    cur.execute("""
        UPDATE transactions
        SET status = 'Completada',
            completed_at = %s
        WHERE id = %s
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

    # =====================
    # FILTERS
    # =====================
    st.markdown("### Filters")

    if not banks_df.empty and "macroproyecto" in banks_df.columns:
        macro_options = ["TODOS"] + sorted(
            banks_df["macroproyecto"].dropna().unique().tolist()
        )
    else:
        macro_options = ["TODOS"]

    selected_macro = st.selectbox(
    "Filter by Macroproyecto",
    macro_options,
    key="dashboard_macroproyecto_filter2"
)

    filtered_banks_df = banks_df.copy()

    if selected_macro != "TODOS" and not filtered_banks_df.empty and "macroproyecto" in filtered_banks_df.columns:
        filtered_banks_df = filtered_banks_df[
            filtered_banks_df["macroproyecto"] == selected_macro
        ].copy()

    if not filtered_banks_df.empty:
        filtered_banks_df["free_volume"] = (
            filtered_banks_df["available_volume"] - filtered_banks_df["reserved_volume"]
        )

    # =====================
    # METRICS
    # =====================
    total_available = float(filtered_banks_df["available_volume"].sum()) if not filtered_banks_df.empty else 0.0
    total_reserved = float(filtered_banks_df["reserved_volume"].sum()) if not filtered_banks_df.empty else 0.0
    total_free = float(filtered_banks_df["free_volume"].sum()) if not filtered_banks_df.empty else 0.0
    total_needed = float(projects_df["missing_volume"].clip(lower=0).sum()) if not projects_df.empty else 0.0

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Available Volume", f"{total_available:,.2f} m³")
    c2.metric("Reserved Volume", f"{total_reserved:,.2f} m³")
    c3.metric("Free Volume", f"{total_free:,.2f} m³")
    c4.metric("Pending Need", f"{total_needed:,.2f} m³")

    # =====================
    # ROW 1: PIE CHARTS
    # =====================
    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### Volume by Quality")

        if not filtered_banks_df.empty:
            quality_chart_df = filtered_banks_df.groupby(
                "quality", as_index=False
            )["available_volume"].sum()

            fig_quality = px.pie(
                quality_chart_df,
                names="quality",
                values="available_volume",
                title="Available Material by Quality"
            )

            fig_quality.update_traces(
                texttemplate="%{value:,.0f} m³ (%{percent})",
                textposition="inside"
            )

            st.plotly_chart(fig_quality, width="stretch")
        else:
            st.info("No banks available for this filter.")

    with col2:
        st.markdown("### Volume by Macroproyecto")

        if not filtered_banks_df.empty and "macroproyecto" in filtered_banks_df.columns:
            macro_chart_df = filtered_banks_df.groupby(
                "macroproyecto", as_index=False
            )["available_volume"].sum()

            fig_macro = px.pie(
                macro_chart_df,
                names="macroproyecto",
                values="available_volume",
                title="Available Volume by Macroproyecto"
            )

            fig_macro.update_traces(
                texttemplate="%{value:,.0f} m³ (%{percent})",
                textposition="inside"
            )

            st.plotly_chart(fig_macro, width="stretch")
        else:
            st.info("No macroproyecto data available.")

    # =====================
    # ROW 2: BAR CHARTS
    # =====================
    col3, col4 = st.columns(2)

    with col3:
        st.markdown("### Volume by Macroproyecto and Quality")

        if not filtered_banks_df.empty and "macroproyecto" in filtered_banks_df.columns:
            stacked_df = filtered_banks_df.groupby(
                ["macroproyecto", "quality"], as_index=False
            )["available_volume"].sum()

            fig_stacked = px.bar(
                stacked_df,
                x="macroproyecto",
                y="available_volume",
                color="quality",
                title="Available Volume by Macroproyecto and Quality",
                barmode="stack"
            )

            st.plotly_chart(fig_stacked, width="stretch")
        else:
            st.info("No macroproyecto data available.")

    with col4:
        st.markdown("### Free Volume by Macroproyecto")

        if not filtered_banks_df.empty and "macroproyecto" in filtered_banks_df.columns:
            free_macro_df = filtered_banks_df.groupby(
                "macroproyecto", as_index=False
            )["free_volume"].sum()

            fig_bar = px.bar(
                free_macro_df,
                x="macroproyecto",
                y="free_volume",
                title="Free Volume by Macroproyecto",
                text="free_volume"
            )

            fig_bar.update_traces(
                texttemplate="%{text:,.0f} m³",
                textposition="outside"
            )

            st.plotly_chart(fig_bar, width="stretch")
        else:
            st.info("No macroproyecto data available.")

    # =====================
    # ROW 3: FILTERED TABLE
    # =====================
    st.markdown("### Banks in Selected Filter")

    if not filtered_banks_df.empty:
        columns_to_show = [
            "id", "name", "macroproyecto", "quality",
            "available_volume", "reserved_volume", "free_volume", "status"
        ]
        existing_columns = [col for col in columns_to_show if col in filtered_banks_df.columns]
        st.dataframe(filtered_banks_df[existing_columns], width="stretch")
    else:
        st.info("No banks found for the selected filter.")

    # =====================
    # ROW 4: RECENT TRANSACTIONS
    # =====================
    st.markdown("### Recent Transactions")

    if not transactions_df.empty:
        st.dataframe(
            transactions_df[[
                "id", "bank_name", "project_name", "volume",
                "quality", "status", "requested_at"
            ]].head(10),
            width="stretch"
        )
    else:
        st.info("No transactions yet.")
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
        macro_options = ["DOMINIO CUMBRES", "TERRA PARK DOMINIO", "DOMINIO HUASTECA", "SIN ASIGNAR"]

        macroproyecto = st.selectbox(
            "Macroproyecto",
            macro_options,
            key="add_bank_macro"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            latitude = st.number_input("Latitude", value=25.7000, format="%.6f")
        with col2:
            longitude = st.number_input("Longitude", value=-100.3000, format="%.6f")

        quality = st.selectbox("Quality", ["Terraplen", "Subrasante", "Lutita", "Triturado"])
        volume = st.number_input("Available volume (m³)", min_value=0.0, value=1000.0, step=100.0)
        status = st.selectbox("Status", ["Disponible", "Reservado", "Agotado"])

        submitted = st.form_submit_button("Add bank")

        if submitted:
            if not name.strip():
                st.error("Please enter a bank name.")
            else:
                add_bank(name.strip(), macroproyecto, latitude, longitude, quality, volume, status)
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
                        st.caption(
    f"{row['macroproyecto']} | {row['quality']} | "
    f"Available: {row['available_volume']:.0f} | Reserved: {row['reserved_volume']:.0f}"
)
        
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
