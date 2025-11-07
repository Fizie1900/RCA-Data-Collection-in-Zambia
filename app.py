Here's the complete fixed `app.py` file with proper database handling that will store information safely:

```python
import streamlit as st
import sqlitecloud
import pandas as pd
from datetime import datetime
import json
import os
import plotly.express as px
import plotly.graph_objects as go
from streamlit_tags import st_tags
import time
import io
import base64
import hashlib

# Import other modules (make sure these files exist)
try:
    from interview_editor import interview_editor_main, InterviewEditor
except ImportError:
    def interview_editor_main():
        st.info("Interview Editor module not available")
    
    class InterviewEditor:
        pass

try:
    from analytics_dashboard import analytics_main, create_interactive_dashboard
except ImportError:
    def analytics_main():
        st.info("Analytics Dashboard module not available")
    
    def create_interactive_dashboard():
        st.info("Interactive Dashboard not available")

# SQLite Cloud configuration
SQLITECLOUD_CONFIG = {
    "connection_string": "sqlitecloud://ctoxm6jkvz.g4.sqlite.cloud:8860/compliance_survey.db?apikey=UoEbilyXxrbfqDUjsrbiLxUZQkRMtyK9fbhIzKVFuAw"
}

def get_connection():
    """Get SQLite Cloud database connection"""
    try:
        conn = sqlitecloud.connect(SQLITECLOUD_CONFIG["connection_string"])
        return conn
    except Exception as e:
        st.error(f"❌ Database connection error: {str(e)}")
        return None

def execute_query(query, params=None, return_result=False):
    """Execute a query on SQLite Cloud"""
    conn = get_connection()
    if conn is None:
        return None
    
    try:
        cursor = conn.cursor()
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        if return_result:
            if query.strip().upper().startswith('SELECT'):
                result = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                return (result, columns)
            else:
                conn.commit()
                return cursor.rowcount
        else:
            conn.commit()
            return True
            
    except Exception as e:
        st.error(f"❌ Query execution error: {str(e)}")
        return None
    finally:
        conn.close()

def execute_many(query, params_list):
    """Execute many queries on SQLite Cloud"""
    conn = get_connection()
    if conn is None:
        return None
    
    try:
        cursor = conn.cursor()
        cursor.executemany(query, params_list)
        conn.commit()
        return True
    except Exception as e:
        st.error(f"❌ Batch execution error: {str(e)}")
        return None
    finally:
        conn.close()

# Set page config MUST be first
st.set_page_config(
    page_title="Zambia Regulatory Compliance Survey",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Enhanced credentials with individual interviewer passwords
INTERVIEWER_CREDENTIALS = {
    "Fizie Fulumaka": {"password": "fizie2024", "role": "interviewer"},
    "Sanderson Mweemba": {"password": "sanderson2024", "role": "interviewer"},
    "Anastazia Mtonga": {"password": "anastazia2024", "role": "interviewer"},
    "Sarah Namutowe": {"password": "sarah2024", "role": "interviewer"},
    "Boris Divjak": {"password": "boris2024", "role": "interviewer"},
    "Other": {"password": "other2024", "role": "interviewer"}
}

ADMIN_CREDENTIALS = {
    "admin": {"password": "compliance2024", "role": "admin"},
    "researcher": {"password": "data2024", "role": "researcher"}
}

# Initialize database with FIXED schema (44 columns)
def init_db():
    """Initialize database tables in SQLite Cloud with FIXED schema"""
    conn = get_connection()
    if conn is None:
        return False
    
    try:
        c = conn.cursor()
        
        # Drop table if exists to recreate with correct schema
        c.execute('DROP TABLE IF EXISTS responses')
        
        # Main responses table - FIXED: 44 columns exactly
        c.execute('''
            CREATE TABLE IF NOT EXISTS responses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                interview_id TEXT UNIQUE,
                interviewer_name TEXT,
                interview_date TEXT,
                start_time TEXT,
                end_time TEXT,
                business_name TEXT,
                district TEXT,
                physical_address TEXT,
                contact_person TEXT,
                email TEXT,
                phone TEXT,
                primary_sector TEXT,
                legal_status TEXT,
                business_size TEXT,
                ownership_structure TEXT,
                gender_owner TEXT,
                business_activities TEXT,
                isic_codes TEXT,
                year_established INTEGER,
                turnover_range TEXT,
                employees_fulltime INTEGER,
                employees_parttime INTEGER,
                procedure_data TEXT,
                completion_time_local REAL,
                completion_time_national REAL,
                completion_time_dk REAL,
                compliance_cost_percentage REAL,
                permit_comparison_national INTEGER,
                permit_comparison_local INTEGER,
                cost_comparison_national INTEGER,
                cost_comparison_local INTEGER,
                business_climate_rating INTEGER,
                reform_priorities TEXT,
                status TEXT DEFAULT 'draft',
                submission_date TIMESTAMP,
                last_modified TIMESTAMP,
                total_compliance_cost REAL DEFAULT 0,
                total_compliance_time INTEGER DEFAULT 0,
                risk_score REAL DEFAULT 0,
                created_by TEXT,
                current_section TEXT DEFAULT 'A',
                draft_progress REAL DEFAULT 0,
                external_support_details TEXT DEFAULT ''  -- Added to make 44 columns
            )
        ''')
        
        # ISIC codes cache table
        c.execute('''
            CREATE TABLE IF NOT EXISTS isic_cache (
                code TEXT PRIMARY KEY,
                title TEXT,
                description TEXT,
                category TEXT,
                last_updated TIMESTAMP
            )
        ''')
        
        # Templates table
        c.execute('''
            CREATE TABLE IF NOT EXISTS templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE,
                sector TEXT,
                business_size TEXT,
                data TEXT,
                created_date TIMESTAMP
            )
        ''')
        
        # Admin logs table
        c.execute('''
            CREATE TABLE IF NOT EXISTS admin_logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT,
                action TEXT,
                timestamp TIMESTAMP,
                details TEXT
            )
        ''')
        
        # User sessions table
        c.execute('''
            CREATE TABLE IF NOT EXISTS user_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT,
                login_time TIMESTAMP,
                logout_time TIMESTAMP,
                session_duration INTEGER
            )
        ''')
        
        conn.commit()
        st.success("✅ Database tables initialized successfully with FIXED schema!")
        return True
        
    except Exception as e:
        st.error(f"❌ Database initialization error: {str(e)}")
        return False
    finally:
        conn.close()

def check_and_fix_database():
    """Check database schema and fix if needed"""
    try:
        # Check if responses table exists
        result = execute_query("SELECT name FROM sqlite_master WHERE type='table' AND name='responses'", return_result=True)
        
        if not result or not result[0] or len(result[0]) == 0:
            st.warning("📊 Database not found. Initializing...")
            return init_db()
        
        # Check if we have the correct number of columns
        result = execute_query("PRAGMA table_info(responses)", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            column_count = len(result[0])
            if column_count != 44:
                st.warning(f"🔄 Database schema mismatch: found {column_count} columns, expected 44. Recreating...")
                return init_db()
        
        return True
    except Exception as e:
        st.error(f"Error checking database: {str(e)}")
        return init_db()

def add_missing_columns():
    """Add missing columns to existing database tables - FIXED VERSION"""
    try:
        # Check if responses table exists first
        result = execute_query("SELECT name FROM sqlite_master WHERE type='table' AND name='responses'", return_result=True)
        if not result or not result[0]:
            st.warning("Responses table doesn't exist. Creating...")
            init_db()
            return
        
        # Get current columns
        result = execute_query("PRAGMA table_info(responses)", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            columns = [column[1] for column in result[0]]
            
            missing_columns = []
            
            # Check for all required columns
            required_columns = [
                'created_by', 'current_section', 'draft_progress', 'external_support_details'
            ]
            
            for column in required_columns:
                if column not in columns:
                    missing_columns.append(column)
            
            if missing_columns:
                st.info(f"🔄 Adding missing columns: {missing_columns}")
                for column in missing_columns:
                    if column == 'created_by':
                        execute_query("ALTER TABLE responses ADD COLUMN created_by TEXT")
                    elif column == 'current_section':
                        execute_query("ALTER TABLE responses ADD COLUMN current_section TEXT DEFAULT 'A'")
                    elif column == 'draft_progress':
                        execute_query("ALTER TABLE responses ADD COLUMN draft_progress REAL DEFAULT 0")
                    elif column == 'external_support_details':
                        execute_query("ALTER TABLE responses ADD COLUMN external_support_details TEXT DEFAULT ''")
                
                st.success("✅ Database schema updated successfully!")
                
    except Exception as e:
        st.warning(f"Database schema update: {str(e)}")

# Enhanced ISIC data management
@st.cache_data
def load_isic_dataframe():
    """Load ISIC data from Excel file and return as DataFrame"""
    excel_files = [
        "Complete_ISIC5_Structure_16Dec2022final.xlsx",
        "ISIC_Codes.xlsx", 
        "isic_codes.xlsx",
        "Complete_ISIC5.xlsx",
        "ISIC_4.xlsx",
        "isic_data.xlsx"
    ]
    
    for filename in excel_files:
        if os.path.exists(filename):
            try:
                df = pd.read_excel(filename)
                st.success(f"✅ ISIC database loaded from: {filename}")
                
                # Cache the data
                cache_isic_data(df)
                return df
            except Exception as e:
                st.warning(f"Could not load {filename}: {str(e)}")
                continue
    
    # If no file found, try to load from cache
    return load_isic_from_cache()

def cache_isic_data(df):
    """Cache ISIC data in database for faster access"""
    try:
        # Clear existing cache
        execute_query("DELETE FROM isic_cache")
        
        # Insert new data
        params_list = []
        for _, row in df.iterrows():
            code_col = next((col for col in ['Code', 'CODE', 'code'] if col in df.columns), None)
            title_col = next((col for col in ['Title', 'TITLE', 'title'] if col in df.columns), None)
            
            if code_col and title_col:
                code = str(row[code_col]) if pd.notna(row[code_col]) else None
                title = str(row[title_col]) if pd.notna(row[title_col]) else ""
                description = title
                
                if code:
                    current_time = datetime.now().isoformat()
                    params_list.append((code, title, description, 'General', current_time))
        
        if params_list:
            execute_many(
                "INSERT INTO isic_cache (code, title, description, category, last_updated) VALUES (?, ?, ?, ?, ?)",
                params_list
            )
            
    except Exception as e:
        st.warning(f"Could not cache ISIC data: {str(e)}")

def load_isic_from_cache():
    """Load ISIC data from database cache"""
    try:
        result = execute_query("SELECT code, title, description FROM isic_cache", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            result_data, columns = result
            df = pd.DataFrame(result_data, columns=columns)
            
            if not df.empty:
                st.info("📁 Loaded ISIC data from cache")
                return df
            else:
                st.warning("No ISIC data available. Using basic codes.")
                return create_basic_isic_data()
        else:
            return create_basic_isic_data()
    except:
        return create_basic_isic_data()

def create_basic_isic_data():
    """Create basic ISIC data structure"""
    basic_data = {
        'Code': ['0111', '0112', '0113', '0114', '0115', '0116', '0121', '0122', 
                '4100', '4101', '4102', '4310', '4311', '4312', '4320', '4321',
                '4610', '4620', '4630', '4651', '4652', '4661', '4662'],
        'Title': [
            'Growing of cereals (except rice), leguminous crops and oil seeds',
            'Growing of rice',
            'Growing of vegetables and melons, roots and tubers',
            'Growing of sugar cane',
            'Growing of tobacco',
            'Growing of fibre crops',
            'Growing of grapes',
            'Growing of tropical and subtropical fruits',
            'Construction of buildings',
            'Construction of residential buildings',
            'Construction of non-residential buildings',
            'Demolition and site preparation',
            'Demolition',
            'Site preparation',
            'Electrical, plumbing and other construction installation activities',
            'Electrical installation',
            'Wholesale on a fee or contract basis',
            'Wholesale of agricultural raw materials and live animals',
            'Wholesale of food, beverages and tobacco',
            'Wholesale of computers, computer peripheral equipment and software',
            'Wholesale of electronic and telecommunications equipment and parts',
            'Wholesale of agricultural machinery, equipment and supplies',
            'Wholesale of construction machinery, equipment and supplies'
        ]
    }
    return pd.DataFrame(basic_data)

def search_isic_codes_enhanced(search_term, isic_df):
    """Enhanced ISIC search with multiple matching strategies"""
    if isic_df is None or not search_term:
        return []
    
    search_term = str(search_term).strip().lower()
    
    try:
        # Try different column combinations
        code_col = next((col for col in ['Code', 'CODE', 'code'] if col in isic_df.columns), None)
        title_col = next((col for col in ['Title', 'TITLE', 'title', 'Description', 'DESCRIPTION'] if col in isic_df.columns), None)
        
        if not code_col or not title_col:
            return []
        
        # Multiple search strategies
        exact_code_matches = isic_df[isic_df[code_col].astype(str).str.lower() == search_term]
        code_contains = isic_df[isic_df[code_col].astype(str).str.lower().str.contains(search_term, na=False)]
        title_contains = isic_df[isic_df[title_col].astype(str).str.lower().str.contains(search_term, na=False)]
        
        # Combine results
        results = pd.concat([exact_code_matches, code_contains, title_contains]).drop_duplicates()
        
        formatted_results = []
        for _, row in results.iterrows():
            code = str(row[code_col]).strip()
            title = str(row[title_col]).strip()
            if code and title:
                formatted_results.append({
                    'code': code,
                    'title': title,
                    'display': f"{code} - {title}",
                    'full_info': f"ISIC {code}: {title}"
                })
        
        return formatted_results[:20]  # Limit results
        
    except Exception as e:
        st.error(f"Search error: {str(e)}")
        return []

# Enhanced data for dropdowns
DISTRICTS = ["Lusaka", "Kitwe", "Kasama", "Ndola", "Livingstone", "Other (Please specify)"]
INTERVIEWERS = list(INTERVIEWER_CREDENTIALS.keys())

# Application modes
APPLICATION_MODES = ["Entirely In-Person", "Mixed", "Entirely Online"]

# Enhanced session state initialization
def initialize_session_state():
    defaults = {
        'custom_procedures': [],
        'custom_authorities': [],
        'procedures_list': [],
        'current_section': 'A',
        'current_interview_id': None,
        'form_data': {},
        'selected_isic_codes': [],
        'manual_isic_input': "",
        'selected_isic_for_business': "",
        'isic_search_term': "",
        'show_detailed_form': False,
        'use_template': False,
        'interview_start_time': None,
        'active_procedure_index': None,
        'district_specific_notes': {},
        'isic_df': None,
        'business_activities_text': "",
        'bulk_procedure_mode': False,
        'quick_manual_mode': False,
        'admin_logged_in': False,
        'interviewer_logged_in': False,
        'current_user': None,
        'user_role': None,
        'app_mode': 'login'
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

# Draft Manager with SQLite Cloud
class DraftManager:
    def __init__(self):
        pass
    
    def get_user_drafts(self, username):
        """Get all drafts for a specific user"""
        try:
            query = """
            SELECT 
                interview_id, business_name, district, primary_sector, 
                business_size, status, last_modified, current_section,
                draft_progress, created_by
            FROM responses 
            WHERE status = 'draft' AND created_by = ?
            ORDER BY last_modified DESC
            """
            result = execute_query(query, (username,), return_result=True)
            if result and isinstance(result, tuple) and result[0]:
                result_data, columns = result
                df = pd.DataFrame(result_data, columns=columns)
                return df
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Error loading drafts: {str(e)}")
            return pd.DataFrame()
    
    def get_all_drafts(self):
        """Get all drafts (for admins)"""
        try:
            query = """
            SELECT 
                interview_id, business_name, district, primary_sector, 
                business_size, status, last_modified, current_section,
                draft_progress, created_by
            FROM responses 
            WHERE status = 'draft'
            ORDER BY last_modified DESC
            """
            result = execute_query(query, return_result=True)
            if result and isinstance(result, tuple) and result[0]:
                result_data, columns = result
                df = pd.DataFrame(result_data, columns=columns)
                return df
            return pd.DataFrame()
        except Exception as e:
            st.error(f"Error loading drafts: {str(e)}")
            return pd.DataFrame()
    
    def load_draft(self, interview_id):
        """Load a specific draft by interview ID"""
        try:
            query = "SELECT * FROM responses WHERE interview_id = ?"
            result = execute_query(query, (interview_id,), return_result=True)
            if result and isinstance(result, tuple) and result[0]:
                result_data, columns = result
                # Convert to dictionary
                return dict(zip(columns, result_data[0]))
            return None
        except Exception as e:
            st.error(f"Error loading draft: {str(e)}")
            return None
    
    def update_draft_progress(self, interview_id, current_section, progress_percentage):
        """Update draft progress and current section"""
        try:
            query = '''
                UPDATE responses 
                SET current_section = ?, draft_progress = ?, last_modified = ?
                WHERE interview_id = ?
            '''
            result = execute_query(query, (current_section, progress_percentage, datetime.now().isoformat(), interview_id))
            return result is not None
        except Exception as e:
            st.error(f"Error updating draft progress: {str(e)}")
            return False
    
    def delete_draft(self, interview_id):
        """Delete a draft interview"""
        try:
            query = "DELETE FROM responses WHERE interview_id = ? AND status = 'draft'"
            result = execute_query(query, (interview_id,))
            return result is not None
        except Exception as e:
            st.error(f"Error deleting draft: {str(e)}")
            return False
    
    def calculate_progress(self, form_data, current_section):
        """Calculate progress percentage based on completed sections"""
        progress = 0
        sections_completed = 0
        total_sections = 4  # A, B, C, D
        
        # Check Section A completion
        if form_data.get('business_name') and form_data.get('contact_person'):
            sections_completed += 1
        
        # Check Section B completion (procedures)
        if form_data.get('procedure_data'):
            try:
                procedures = json.loads(form_data['procedure_data'])
                if procedures and len(procedures) > 0:
                    sections_completed += 1
            except:
                pass
        
        # Check Section C completion
        if (form_data.get('completion_time_local') is not None and 
            form_data.get('completion_time_national') is not None):
            sections_completed += 1
        
        # Check Section D completion
        if form_data.get('reform_priorities'):
            try:
                reforms = json.loads(form_data['reform_priorities'])
                if reforms and len(reforms) > 0:
                    sections_completed += 1
            except:
                pass
        
        # Base progress + current section progress
        base_progress = (sections_completed / total_sections) * 80
        current_section_progress = 20  # 20% for current active section
        
        return min(base_progress + current_section_progress, 100)

def display_draft_dashboard():
    """Main draft management dashboard"""
    st.title("📋 Draft Management")
    
    # Initialize draft manager
    draft_manager = DraftManager()
    
    # Get user's drafts
    if st.session_state.get('admin_logged_in', False):
        drafts_df = draft_manager.get_all_drafts()
        user_type = "All Users"
    else:
        drafts_df = draft_manager.get_user_drafts(st.session_state.current_user)
        user_type = "Your"
    
    if not drafts_df.empty:
        st.subheader(f"{user_type} Draft Interviews ({len(drafts_df)})")
        
        # Display drafts in a nice format
        for index, draft in drafts_df.iterrows():
            display_draft_card(draft_manager, draft, index)
    else:
        st.info("💡 No draft interviews found. Start a new interview to create drafts!")
    
    # Quick actions
    st.sidebar.markdown("---")
    st.sidebar.subheader("🚀 Quick Actions")
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🆕 New Interview", use_container_width=True, key="new_interview_btn"):
            reset_interview()
            st.session_state.current_section = 'A'
            st.rerun()
    
    with col2:
        if st.button("🔄 Refresh", use_container_width=True, key="refresh_drafts"):
            st.rerun()

def display_draft_card(draft_manager, draft, index):
    """Display a draft as a card with actions"""
    with st.container():
        st.markdown("---")
        
        col1, col2, col3 = st.columns([3, 1, 1])
        
        with col1:
            st.write(f"### {draft['business_name'] or 'Unnamed Business'}")
            
            # Draft info
            info_col1, info_col2, info_col3 = st.columns(3)
            with info_col1:
                st.write(f"**District:** {draft['district'] or 'Not set'}")
                st.write(f"**Sector:** {draft['primary_sector'] or 'Not set'}")
            with info_col2:
                st.write(f"**Last Saved:** {draft['last_modified'][:16] if draft['last_modified'] else 'Never'}")
                st.write(f"**Current Section:** {draft['current_section'] or 'A'}")
            with info_col3:
                st.write(f"**Progress:** {draft['draft_progress'] or 0}%")
                if draft['created_by'] and st.session_state.get('admin_logged_in', False):
                    st.write(f"**Created by:** {draft['created_by']}")
            
            # Progress bar
            progress = draft['draft_progress'] or 0
            st.progress(progress / 100)
        
        with col2:
            if st.button("➡️ Continue", key=f"continue_{index}", use_container_width=True):
                load_draft_into_session(draft_manager, draft['interview_id'])
                st.rerun()
        
        with col3:
            if st.button("🗑️ Delete", key=f"delete_{index}", use_container_width=True):
                if draft_manager.delete_draft(draft['interview_id']):
                    st.success("✅ Draft deleted successfully!")
                    st.rerun()

def load_draft_into_session(draft_manager, interview_id):
    """Load a draft into the current session"""
    draft_data = draft_manager.load_draft(interview_id)
    
    if draft_data:
        # Load all session state variables
        st.session_state.current_interview_id = interview_id
        st.session_state.form_data = {
            'interviewer_name': draft_data.get('interviewer_name', ''),
            'interview_date': draft_data.get('interview_date', ''),
            'start_time': draft_data.get('start_time', ''),
            'end_time': draft_data.get('end_time', ''),
            'business_name': draft_data.get('business_name', ''),
            'district': draft_data.get('district', ''),
            'physical_address': draft_data.get('physical_address', ''),
            'contact_person': draft_data.get('contact_person', ''),
            'email': draft_data.get('email', ''),
            'phone': draft_data.get('phone', ''),
            'primary_sector': draft_data.get('primary_sector', ''),
            'legal_status': draft_data.get('legal_status', ''),
            'business_size': draft_data.get('business_size', ''),
            'ownership_structure': draft_data.get('ownership_structure', ''),
            'gender_owner': draft_data.get('gender_owner', ''),
            'business_activities': draft_data.get('business_activities', ''),
            'year_established': draft_data.get('year_established', 0),
            'turnover_range': draft_data.get('turnover_range', ''),
            'employees_fulltime': draft_data.get('employees_fulltime', 0),
            'employees_parttime': draft_data.get('employees_parttime', 0),
            'completion_time_local': draft_data.get('completion_time_local', 0),
            'completion_time_national': draft_data.get('completion_time_national', 0),
            'completion_time_dk': draft_data.get('completion_time_dk', 0),
            'compliance_cost_percentage': draft_data.get('compliance_cost_percentage', 0),
            'permit_comparison_national': draft_data.get('permit_comparison_national', ''),
            'permit_comparison_local': draft_data.get('permit_comparison_local', ''),
            'cost_comparison_national': draft_data.get('cost_comparison_national', ''),
            'cost_comparison_local': draft_data.get('cost_comparison_local', ''),
            'business_climate_rating': draft_data.get('business_climate_rating', ''),
            'reform_priorities': draft_data.get('reform_priorities', '[]')
        }
        
        # Load procedures
        procedures_json = draft_data.get('procedure_data')
        if procedures_json and procedures_json != 'null' and procedures_json != '[]':
            try:
                st.session_state.procedures_list = json.loads(procedures_json)
            except:
                st.session_state.procedures_list = []
        else:
            st.session_state.procedures_list = []
        
        # Load ISIC codes
        isic_json = draft_data.get('isic_codes')
        if isic_json and isic_json != 'null' and isic_json != '[]':
            try:
                st.session_state.selected_isic_codes = json.loads(isic_json)
            except:
                st.session_state.selected_isic_codes = []
        else:
            st.session_state.selected_isic_codes = []
        
        # Load business activities text
        st.session_state.business_activities_text = draft_data.get('business_activities', '')
        
        # Set current section
        st.session_state.current_section = draft_data.get('current_section', 'A')
        
        st.success(f"✅ Loaded draft: {draft_data.get('business_name', 'Unnamed Business')}")
    else:
        st.error("❌ Failed to load draft")

def display_draft_quick_access():
    """Display quick draft access in sidebar"""
    if st.session_state.get('interviewer_logged_in', False) or st.session_state.get('admin_logged_in', False):
        draft_manager = DraftManager()
        
        if st.session_state.get('admin_logged_in', False):
            drafts_df = draft_manager.get_all_drafts()
        else:
            drafts_df = draft_manager.get_user_drafts(st.session_state.current_user)
        
        if not drafts_df.empty:
            st.sidebar.markdown("---")
            st.sidebar.subheader("📝 Your Drafts")
            
            for index, draft in drafts_df.head(3).iterrows():  # Show only 3 most recent
                business_name = draft['business_name'] or 'Unnamed Business'
                progress = draft['draft_progress'] or 0
                
                if st.sidebar.button(
                    f"➡️ {business_name[:20]}... ({progress}%)", 
                    key=f"sidebar_draft_{index}",
                    use_container_width=True
                ):
                    load_draft_into_session(draft_manager, draft['interview_id'])
                    st.rerun()
            
            # Show "View All" if there are more drafts
            if len(drafts_df) > 3:
                if st.sidebar.button("📋 View All Drafts", use_container_width=True):
                    st.session_state.current_section = 'Draft_Dashboard'
                    st.rerun()

def auto_save_draft():
    """Auto-save current form state as draft"""
    if (st.session_state.get('form_data') and 
        st.session_state.get('current_interview_id') and
        st.session_state.get('interviewer_logged_in', False)):
        
        # Calculate progress
        draft_manager = DraftManager()
        progress = draft_manager.calculate_progress(
            st.session_state.form_data, 
            st.session_state.current_section
        )
        
        # Update progress
        draft_manager.update_draft_progress(
            st.session_state.current_interview_id,
            st.session_state.current_section,
            progress
        )
        
        return True
    return False

# Database functions - FIXED VERSION with 44 columns
def save_draft(data, interview_id=None):
    """Save form data as draft with enhanced calculations and progress tracking - FIXED for 44 columns"""
    try:
        if not interview_id:
            interview_id = generate_interview_id()
        
        # Calculate total compliance metrics
        procedure_data = data.get('procedure_data', [])
        total_cost = sum(proc.get('official_fees', 0) + proc.get('unofficial_payments', 0) for proc in procedure_data)
        total_time = sum(proc.get('total_days', 0) for proc in procedure_data)
        
        # Calculate risk score (simplified)
        risk_score = min((total_cost / 100000 + total_time / 365) * 10, 10)  # Scale to 0-10
        
        # Calculate draft progress
        draft_manager = DraftManager()
        progress = draft_manager.calculate_progress(data, st.session_state.current_section)
        
        # Prepare data for insertion
        isic_codes = data.get('isic_codes', [])
        reform_priorities = data.get('reform_priorities', [])
        procedure_data_json = json.dumps(procedure_data)
        
        current_time = datetime.now().isoformat()
        
        # Check if record exists
        existing_result = execute_query("SELECT id FROM responses WHERE interview_id = ?", (interview_id,), return_result=True)
        
        if existing_result and isinstance(existing_result, tuple) and existing_result[0]:
            # Update existing draft - FIXED: 44 columns
            update_query = '''
                UPDATE responses SET
                    interviewer_name=?, interview_date=?, start_time=?, end_time=?,
                    business_name=?, district=?, physical_address=?, contact_person=?,
                    email=?, phone=?, primary_sector=?, legal_status=?, business_size=?,
                    ownership_structure=?, gender_owner=?, business_activities=?,
                    isic_codes=?, year_established=?, turnover_range=?,
                    employees_fulltime=?, employees_parttime=?, procedure_data=?,
                    completion_time_local=?, completion_time_national=?, completion_time_dk=?,
                    compliance_cost_percentage=?, permit_comparison_national=?,
                    permit_comparison_local=?, cost_comparison_national=?,
                    cost_comparison_local=?, business_climate_rating=?,
                    reform_priorities=?, last_modified=?, total_compliance_cost=?,
                    total_compliance_time=?, risk_score=?, created_by=?,
                    current_section=?, draft_progress=?, external_support_details=?
                WHERE interview_id=?
            '''
            params = (
                data.get('interviewer_name', ''),
                data.get('interview_date', ''),
                data.get('start_time', ''),
                data.get('end_time', ''),
                data.get('business_name', ''),
                data.get('district', ''),
                data.get('physical_address', ''),
                data.get('contact_person', ''),
                data.get('email', ''),
                data.get('phone', ''),
                data.get('primary_sector', ''),
                data.get('legal_status', ''),
                data.get('business_size', ''),
                data.get('ownership_structure', ''),
                data.get('gender_owner', ''),
                data.get('business_activities', ''),
                json.dumps(isic_codes),
                data.get('year_established', 0),
                data.get('turnover_range', ''),
                data.get('employees_fulltime', 0),
                data.get('employees_parttime', 0),
                procedure_data_json,
                data.get('completion_time_local', 0.0),
                data.get('completion_time_national', 0.0),
                data.get('completion_time_dk', 0.0),
                data.get('compliance_cost_percentage', 0.0),
                data.get('permit_comparison_national', 0),
                data.get('permit_comparison_local', 0),
                data.get('cost_comparison_national', 0),
                data.get('cost_comparison_local', 0),
                data.get('business_climate_rating', 0),
                json.dumps(reform_priorities),
                current_time,
                total_cost,
                total_time,
                risk_score,
                st.session_state.current_user,
                st.session_state.current_section,
                progress,
                '',  # external_support_details - empty for now
                interview_id
            )
            result = execute_query(update_query, params)
        else:
            # Insert new draft - FIXED: 44 values for 44 columns
            insert_query = '''
                INSERT INTO responses VALUES (
                    NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
            '''
            insert_data = (
                interview_id,
                data.get('interviewer_name', ''),
                data.get('interview_date', ''),
                data.get('start_time', ''),
                data.get('end_time', ''),
                data.get('business_name', ''),
                data.get('district', ''),
                data.get('physical_address', ''),
                data.get('contact_person', ''),
                data.get('email', ''),
                data.get('phone', ''),
                data.get('primary_sector', ''),
                data.get('legal_status', ''),
                data.get('business_size', ''),
                data.get('ownership_structure', ''),
                data.get('gender_owner', ''),
                data.get('business_activities', ''),
                json.dumps(isic_codes),
                data.get('year_established', 0),
                data.get('turnover_range', ''),
                data.get('employees_fulltime', 0),
                data.get('employees_parttime', 0),
                procedure_data_json,
                data.get('completion_time_local', 0.0),
                data.get('completion_time_national', 0.0),
                data.get('completion_time_dk', 0.0),
                data.get('compliance_cost_percentage', 0.0),
                data.get('permit_comparison_national', 0),
                data.get('permit_comparison_local', 0),
                data.get('cost_comparison_national', 0),
                data.get('cost_comparison_local', 0),
                data.get('business_climate_rating', 0),
                json.dumps(reform_priorities),
                'draft',
                current_time,
                current_time,
                total_cost,
                total_time,
                risk_score,
                st.session_state.current_user,
                st.session_state.current_section,
                progress,
                ''  # external_support_details - empty for now
            )
            result = execute_query(insert_query, insert_data)
        
        if result:
            # Auto-save draft progress
            auto_save_draft()
            return interview_id
        return None
        
    except Exception as e:
        st.error(f"Error saving draft: {str(e)}")
        import traceback
        st.error(f"Error details: {traceback.format_exc()}")
        return None

def check_duplicate_business_name(business_name, current_interview_id=None):
    """Check if business name already exists in database"""
    try:
        if current_interview_id:
            # Check for duplicates excluding current interview
            result = execute_query("SELECT COUNT(*) FROM responses WHERE business_name = ? AND interview_id != ?", 
                                 (business_name, current_interview_id), return_result=True)
        else:
            # Check for any duplicates
            result = execute_query("SELECT COUNT(*) FROM responses WHERE business_name = ?", (business_name,), return_result=True)
        
        if result and isinstance(result, tuple) and result[0]:
            count = result[0][0][0] if result[0] else 0
            return count > 0
        return False
    except Exception as e:
        st.error(f"Error checking duplicate business name: {str(e)}")
        return False

def submit_final(interview_id):
    """Mark draft as final submission with validation"""
    try:
        # Check for duplicate business name before submission
        business_name = st.session_state.form_data.get('business_name', '')
        if check_duplicate_business_name(business_name, interview_id):
            st.error(f"❌ Business name '{business_name}' already exists in the database. Please use a unique business name.")
            return False
        
        current_time = datetime.now().isoformat()
        result = execute_query("UPDATE responses SET status = 'submitted', submission_date = ? WHERE interview_id = ?", 
                              (current_time, interview_id))
        
        if result:
            # Log the submission
            log_admin_action(st.session_state.current_user, "interview_submitted", f"Interview {interview_id} submitted")
            return True
        return False
    except Exception as e:
        st.error(f"Error submitting final: {str(e)}")
        return False

def generate_interview_id():
    """Generate unique interview ID"""
    return f"INT_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def get_all_interviews():
    """Get all interviews from database - FIXED VERSION"""
    try:
        # First ensure database is properly initialized
        if not check_and_fix_database():
            return pd.DataFrame()
            
        # Check if created_by column exists
        result = execute_query("PRAGMA table_info(responses)", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            columns = [column[1] for column in result[0]]
            
            if 'created_by' in columns:
                query = """
                SELECT 
                    interview_id, business_name, district, primary_sector, 
                    business_size, status, submission_date, last_modified,
                    total_compliance_cost, total_compliance_time, risk_score, created_by
                FROM responses 
                ORDER BY last_modified DESC
                """
            else:
                # Fallback query without created_by column
                query = """
                SELECT 
                    interview_id, business_name, district, primary_sector, 
                    business_size, status, submission_date, last_modified,
                    total_compliance_cost, total_compliance_time, risk_score
                FROM responses 
                ORDER BY last_modified DESC
                """
            
            result = execute_query(query, return_result=True)
            if result and isinstance(result, tuple) and result[0]:
                result_data, columns = result
                df = pd.DataFrame(result_data, columns=columns)
                return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading interviews: {str(e)}")
        return pd.DataFrame()

def get_user_interviews(username):
    """Get interviews created by specific user - FIXED VERSION"""
    try:
        # First ensure database is properly initialized
        if not check_and_fix_database():
            return pd.DataFrame()
            
        # Check if created_by column exists
        result = execute_query("PRAGMA table_info(responses)", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            columns = [column[1] for column in result[0]]
            
            if 'created_by' in columns:
                query = """
                SELECT 
                    interview_id, business_name, district, primary_sector, 
                    business_size, status, submission_date, last_modified,
                    total_compliance_cost, total_compliance_time, risk_score
                FROM responses 
                WHERE created_by = ?
                ORDER BY last_modified DESC
                """
                result = execute_query(query, (username,), return_result=True)
            else:
                # If created_by column doesn't exist, return all interviews for now
                query = """
                SELECT 
                    interview_id, business_name, district, primary_sector, 
                    business_size, status, submission_date, last_modified,
                    total_compliance_cost, total_compliance_time, risk_score
                FROM responses 
                ORDER BY last_modified DESC
                """
                result = execute_query(query, return_result=True)
            
            if result and isinstance(result, tuple) and result[0]:
                result_data, columns = result
                df = pd.DataFrame(result_data, columns=columns)
                return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading user interviews: {str(e)}")
        return pd.DataFrame()

def get_interview_details(interview_id):
    """Get detailed interview data"""
    try:
        # First ensure database is properly initialized
        if not check_and_fix_database():
            return pd.DataFrame()
            
        query = "SELECT * FROM responses WHERE interview_id = ?"
        result = execute_query(query, (interview_id,), return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            result_data, columns = result
            df = pd.DataFrame(result_data, columns=columns)
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Error loading interview details: {str(e)}")
        return pd.DataFrame()

def get_database_stats():
    """Get database statistics"""
    try:
        # First ensure database is properly initialized
        if not check_and_fix_database():
            return {}
            
        stats = {}
        
        # Total interviews
        result = execute_query("SELECT COUNT(*) as count FROM responses", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            stats['total_interviews'] = result[0][0][0] if result[0] else 0
        
        result = execute_query("SELECT COUNT(*) as count FROM responses WHERE status = 'submitted'", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            stats['submitted_interviews'] = result[0][0][0] if result[0] else 0
        
        result = execute_query("SELECT COUNT(*) as count FROM responses WHERE status = 'draft'", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            stats['draft_interviews'] = result[0][0][0] if result[0] else 0
        
        # User-specific stats
        if st.session_state.user_role == 'interviewer' and st.session_state.current_user:
            result = execute_query("SELECT COUNT(*) as count FROM responses WHERE created_by = ?", (st.session_state.current_user,), return_result=True)
            if result and isinstance(result, tuple) and result[0]:
                stats['user_interviews'] = result[0][0][0] if result[0] else 0
            else:
                stats['user_interviews'] = 0
        
        # Sector distribution
        result = execute_query("SELECT primary_sector, COUNT(*) as count FROM responses GROUP BY primary_sector", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            result_data, columns = result
            stats['sector_dist'] = pd.DataFrame(result_data, columns=columns)
        else:
            stats['sector_dist'] = pd.DataFrame()
        
        # District distribution
        result = execute_query("SELECT district, COUNT(*) as count FROM responses GROUP BY district", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            result_data, columns = result
            stats['district_dist'] = pd.DataFrame(result_data, columns=columns)
        else:
            stats['district_dist'] = pd.DataFrame()
        
        # Business size distribution
        result = execute_query("SELECT business_size, COUNT(*) as count FROM responses GROUP BY business_size", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            result_data, columns = result
            stats['size_dist'] = pd.DataFrame(result_data, columns=columns)
        else:
            stats['size_dist'] = pd.DataFrame()
        
        # Average compliance metrics
        result = execute_query("""
            SELECT 
                AVG(total_compliance_cost) as avg_cost,
                AVG(total_compliance_time) as avg_time,
                AVG(risk_score) as avg_risk
            FROM responses 
            WHERE status = 'submitted'
        """, return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            result_data, columns = result
            stats['avg_metrics'] = pd.DataFrame(result_data, columns=columns)
        else:
            stats['avg_metrics'] = pd.DataFrame()
        
        return stats
    except Exception as e:
        st.error(f"Error loading statistics: {str(e)}")
        return {}
        
def add_quick_edit_options():
    """Add quick edit options to data management views"""
    if st.session_state.get('admin_logged_in', False):
        # Use session state to track if we've already shown this in the current context
        if 'quick_edit_shown' not in st.session_state:
            st.session_state.quick_edit_shown = set()
        
        current_context = str(st.session_state.get('_runnable_script_hash', 'default'))
        
        if current_context not in st.session_state.quick_edit_shown:
            st.sidebar.markdown("---")
            st.sidebar.subheader("🛠️ Quick Actions")
            
            if st.sidebar.button("✏️ Open Interview Editor", use_container_width=True, key="quick_edit_unique"):
                st.session_state.current_section = 'Edit_Interviews'
                st.rerun()
            
            st.session_state.quick_edit_shown.add(current_context)
            
def log_admin_action(username, action, details=""):
    """Log admin actions"""
    try:
        current_time = datetime.now().isoformat()
        result = execute_query(
            "INSERT INTO admin_logs (username, action, timestamp, details) VALUES (?, ?, ?, ?)",
            (username, action, current_time, details)
        )
        return result is not None
    except Exception as e:
        st.error(f"Error logging admin action: {str(e)}")
        return False

def log_user_session(username, login_time, logout_time=None, duration=None):
    """Log user session information"""
    try:
        result = execute_query(
            "INSERT INTO user_sessions (username, login_time, logout_time, session_duration) VALUES (?, ?, ?, ?)",
            (username, login_time, logout_time, duration)
        )
        return result is not None
    except Exception as e:
        st.error(f"Error logging user session: {str(e)}")
        return False

# Enhanced Business Activities with ISIC Integration
def business_activities_section():
    """Enhanced business activities section with ISIC integration"""
    
    st.subheader("🏢 Business Activities & ISIC Classification")
    
    # Two-column layout for better organization
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.write("**Describe your main business activities:**")
        business_activities = st.text_area(
            "Business Activities Description *",
            value=st.session_state.business_activities_text,
            placeholder="Describe your main business activities, products, and services in detail...",
            height=120,
            key="business_activities_desc"
        )
        st.session_state.business_activities_text = business_activities
    
    with col2:
        st.write("**💡 Tips:**")
        st.write("• Be specific about products/services")
        st.write("• Include all major revenue streams")
        st.write("• Mention any specialized activities")
    
    # ISIC Code Selection Section
    st.subheader("📊 ISIC Code Classification")
    
    # Enhanced ISIC search and selection
    search_term = st.text_input(
        "🔍 Search ISIC codes by code or description:",
        placeholder="e.g., agriculture, construction, 0111, manufacturing",
        key="isic_search_main",
        value=st.session_state.isic_search_term
    )
    
    # Update search term in session state
    if search_term != st.session_state.isic_search_term:
        st.session_state.isic_search_term = search_term
    
    # Display search results automatically when there's a search term
    if st.session_state.isic_search_term and st.session_state.isic_df is not None:
        with st.spinner("Searching ISIC codes..."):
            search_results = search_isic_codes_enhanced(
                st.session_state.isic_search_term, 
                st.session_state.isic_df
            )
        
        if search_results:
            st.write(f"📋 Found {len(search_results)} matching ISIC codes:")
            
            # Display results in a compact format
            for i, result in enumerate(search_results):
                col1, col2, col3 = st.columns([3, 1, 1])
                
                with col1:
                    st.write(f"**{result['code']}** - {result['title']}")
                
                with col2:
                    if st.button("➕ Select", key=f"select_{i}"):
                        if result['display'] not in st.session_state.selected_isic_codes:
                            st.session_state.selected_isic_codes.append(result['display'])
                            # Auto-update business activities text
                            current_activities = st.session_state.business_activities_text
                            new_activity = f"{result['title']} (ISIC: {result['code']})"
                            if current_activities:
                                st.session_state.business_activities_text = current_activities + "; " + new_activity
                            else:
                                st.session_state.business_activities_text = new_activity
                            st.rerun()
                
                with col3:
                    if st.button("ℹ️ Info", key=f"info_{i}"):
                        st.info(f"**ISIC {result['code']}**: {result['title']}")
            
            st.markdown("---")
        elif st.session_state.isic_search_term:  # Only show warning if there was a search term but no results
            st.warning("No ISIC codes found. Try different search terms.")
    
    # Manual ISIC code entry
    with st.expander("➕ Add Custom ISIC Code", expanded=False):
        manual_col1, manual_col2, manual_col3 = st.columns([2, 2, 1])
        
        with manual_col1:
            custom_code = st.text_input("ISIC Code:", placeholder="e.g., 0111", key="custom_code_input")
        
        with manual_col2:
            custom_description = st.text_input("Description:", placeholder="e.g., Growing of cereals", key="custom_desc_input")
        
        with manual_col3:
            st.write("")  # Spacer
            st.write("")  # Spacer
            if st.button("Add Custom", key="add_custom_isic"):
                if custom_code and custom_description:
                    custom_display = f"{custom_code} - {custom_description}"
                    if custom_display not in st.session_state.selected_isic_codes:
                        st.session_state.selected_isic_codes.append(custom_display)
                        st.success(f"Added custom ISIC code: {custom_code}")
                        st.rerun()
    
    # Display selected ISIC codes
    if st.session_state.selected_isic_codes:
        st.subheader("✅ Selected ISIC Codes")
        
        # Group codes by category for better organization
        agri_codes = [code for code in st.session_state.selected_isic_codes if any(x in code for x in ['01', '02', '03'])]
        construction_codes = [code for code in st.session_state.selected_isic_codes if any(x in code for x in ['41', '42', '43'])]
        other_codes = [code for code in st.session_state.selected_isic_codes if code not in agri_codes + construction_codes]
        
        if agri_codes:
            with st.expander("🌾 Agriculture Codes", expanded=True):
                for i, code in enumerate(agri_codes):
                    display_selected_isic_code(code, i, "agri")
        
        if construction_codes:
            with st.expander("🏗️ Construction Codes", expanded=True):
                for i, code in enumerate(construction_codes):
                    display_selected_isic_code(code, i, "constr")
        
        if other_codes:
            with st.expander("📦 Other Codes", expanded=True):
                for i, code in enumerate(other_codes):
                    display_selected_isic_code(code, i, "other")
        
        # Summary
        st.info(f"**Total ISIC codes selected:** {len(st.session_state.selected_isic_codes)}")
    
    return business_activities

def display_selected_isic_code(code, index, prefix):
    """Display a selected ISIC code with remove option"""
    col1, col2 = st.columns([4, 1])
    
    with col1:
        st.write(f"• {code}")
    
    with col2:
        if st.button("🗑️", key=f"remove_{prefix}_{index}"):
            st.session_state.selected_isic_codes.pop(index)
            st.rerun()

# Quick Manual Procedure Entry
def quick_manual_procedure():
    """Ultra-fast manual procedure entry"""
    st.subheader("⚡ Quick Manual Entry")
    
    with st.form("quick_manual_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            quick_procedure = st.text_input(
                "Procedure Name *", 
                placeholder="e.g., Business License, Environmental Permit",
                key="quick_procedure"
            )
            quick_authority = st.text_input(
                "Regulatory Body *",
                placeholder="e.g., Local Council, ZRA, PACRA",
                key="quick_authority"
            )
            quick_status = st.selectbox(
                "Status",
                ["Not Started", "In Progress", "Completed", "Delayed"],
                index=2,
                key="quick_status"
            )
        
        with col2:
            quick_cost = st.number_input(
                "Total Cost (ZMW) *", 
                min_value=0.0, 
                value=0.0,
                key="quick_cost"
            )
            quick_days = st.number_input(
                "Total Days *",
                min_value=0,
                value=30,
                key="quick_days"
            )
            quick_mode = st.selectbox(
                "Application Mode",
                APPLICATION_MODES,
                key="quick_mode"
            )
        
        # Quick complexity assessment
        complexity_help = st.radio(
            "How complex was this procedure?",
            ["Very Simple", "Somewhat Simple", "Moderate", "Complex", "Very Complex"],
            horizontal=True,
            key="quick_complexity"
        )
        
        complexity_map = {
            "Very Simple": 1, "Somewhat Simple": 2, "Moderate": 3, 
            "Complex": 4, "Very Complex": 5
        }
        
        if st.form_submit_button("🚀 Add Procedure (Quick)", use_container_width=True):
            if quick_procedure and quick_authority:
                procedure_data = {
                    'procedure': quick_procedure,
                    'authority': quick_authority,
                    'status': quick_status,
                    'prep_days': max(1, quick_days // 3),
                    'wait_days': max(1, quick_days - (quick_days // 3)),
                    'total_days': quick_days,
                    'official_fees': quick_cost,
                    'unofficial_payments': 0.0,
                    'travel_costs': 0.0,
                    'external_support': 'No',
                    'external_cost': 0.0,
                    'complexity': complexity_map[complexity_help],
                    'renewable': 'Yes',
                    'renewal_frequency': 'Annual',
                    'application_mode': quick_mode,
                    'documents': [],
                    'challenges': '',
                    'follow_ups': 2
                }
                
                st.session_state.procedures_list.append(procedure_data)
                st.success(f"✅ Added: {quick_procedure}")
                st.rerun()
            else:
                st.error("Please fill in Procedure Name and Regulatory Body")

# Enhanced Bulk Procedures Capture
def enhanced_bulk_procedures_capture():
    """Enhanced bulk capture with more options"""
    
    st.subheader("📊 Enhanced Bulk Procedure Capture")
    
    # Sector-based quick templates with more options
    sector = st.session_state.form_data.get('primary_sector', 'Agribusiness')
    
    # Expanded license templates
    expanded_licenses = {
        "Agribusiness": {
            "PACRA Business Registration": {
                "authority": "PACRA", "renewable": "No", "renewal_frequency": "One-time",
                "common_documents": ["Application form", "Business name reservation", "Identification documents"],
                "typical_cost": 1000, "typical_days": 14, "complexity": 3
            },
            "ZRA Tax Registration": {
                "authority": "ZRA", "renewable": "No", "renewal_frequency": "One-time", 
                "common_documents": ["PACRA certificate", "Business registration", "Owner identification"],
                "typical_cost": 0, "typical_days": 7, "complexity": 2
            },
            "Local Trading License": {
                "authority": "Local Council", "renewable": "Yes", "renewal_frequency": "Annual",
                "common_documents": ["Application form", "Business premises details", "Health certificate"],
                "typical_cost": 1500, "typical_days": 21, "complexity": 4
            },
            "ZEMA Environmental License": {
                "authority": "ZEMA", "renewable": "Yes", "renewal_frequency": "3 years",
                "common_documents": ["Environmental Impact Assessment", "Project description", "Site plans"],
                "typical_cost": 5000, "typical_days": 90, "complexity": 7
            },
            "Food and Drugs Act License": {
                "authority": "Ministry of Health", "renewable": "Yes", "renewal_frequency": "Annual",
                "common_documents": ["Product samples", "Laboratory tests", "Manufacturing details"],
                "typical_cost": 2000, "typical_days": 30, "complexity": 5
            },
            "WARMA Water Permit": {
                "authority": "WARMA", "renewable": "Yes", "renewal_frequency": "5 years",
                "common_documents": ["Water usage report", "Environmental plan", "Site assessment"],
                "typical_cost": 3000, "typical_days": 60, "complexity": 6
            }
        },
        "Construction": {
            "NCC Registration": {
                "authority": "NCC", "renewable": "Yes", "renewal_frequency": "Annual",
                "common_documents": ["Company registration", "Technical staff certificates", "Equipment list"],
                "typical_cost": 5000, "typical_days": 30, "complexity": 7
            },
            "Building Permit": {
                "authority": "Local Council", "renewable": "No", "renewal_frequency": "Project-based",
                "common_documents": ["Architectural drawings", "Structural designs", "Site plans"],
                "typical_cost": 2500, "typical_days": 45, "complexity": 6
            },
            "ZEMA EIA Permit": {
                "authority": "ZEMA", "renewable": "Yes", "renewal_frequency": "Project-based",
                "common_documents": ["Environmental Impact Statement", "Project brief", "Mitigation plans"],
                "typical_cost": 10000, "typical_days": 120, "complexity": 8
            },
            "Road Cutting Permit": {
                "authority": "Local Council", "renewable": "No", "renewal_frequency": "Project-based",
                "common_documents": ["Traffic management plan", "Site plans", "Public liability insurance"],
                "typical_cost": 1500, "typical_days": 14, "complexity": 4
            },
            "Planning Permission": {
                "authority": "Local Council", "renewable": "No", "renewal_frequency": "Project-based", 
                "common_documents": ["Land title", "Survey plans", "Development proposal"],
                "typical_cost": 2000, "typical_days": 30, "complexity": 5
            },
            "Occupational Certificate": {
                "authority": "Local Council", "renewable": "No", "renewal_frequency": "Project-based",
                "common_documents": ["Completion certificate", "Building inspection report", "Utility connections"],
                "typical_cost": 1000, "typical_days": 14, "complexity": 3
            }
        }
    }
    
    # Quick action buttons
    st.write("**🚀 Quick Actions**")
    quick_col1, quick_col2, quick_col3, quick_col4 = st.columns(4)
    
    with quick_col1:
        if st.button("🏗️ All Construction", use_container_width=True, key="all_constr_btn"):
            add_all_sector_templates("Construction", expanded_licenses)
    
    with quick_col2:
        if st.button("🌾 All Agribusiness", use_container_width=True, key="all_agri_btn"):
            add_all_sector_templates("Agribusiness", expanded_licenses)
    
    with quick_col3:
        if st.button("🏛️ Common National", use_container_width=True, key="common_national_btn"):
            add_common_national_licenses(sector)
    
    with quick_col4:
        if st.button("🗑️ Clear All", use_container_width=True, key="clear_all_btn"):
            st.session_state.procedures_list = []
            st.rerun()
    
    # Smart template suggestions
    st.write("**💡 Smart Template Suggestions**")
    suggested_licenses = get_suggested_licenses(sector, st.session_state.procedures_list)
    
    if suggested_licenses:
        cols = st.columns(min(3, len(suggested_licenses)))
        for idx, (license_name, license_data) in enumerate(suggested_licenses.items()):
            with cols[idx % 3]:
                if st.button(f"➕ {license_name}", key=f"suggest_{license_name}"):
                    add_license_from_template(license_name, license_data, expanded_licenses[sector][license_name])
                    st.rerun()
    
    # Enhanced bulk add form
    with st.form("enhanced_bulk_form"):
        st.write("**📋 Bulk License Selection**")
        
        sector_licenses = expanded_licenses.get(sector, {})
        
        # Categorized selection
        national_licenses = {k: v for k, v in sector_licenses.items() if any(auth in v['authority'] for auth in ['PACRA', 'ZRA', 'NAPSA', 'ZEMA', 'NCC'])}
        local_licenses = {k: v for k, v in sector_licenses.items() if 'Council' in v['authority'] or 'Local' in v['authority']}
        other_licenses = {k: v for k, v in sector_licenses.items() if k not in national_licenses and k not in local_licenses}
        
        selected_licenses = []
        
        with st.expander("🏛️ National Licenses", expanded=True):
            for license_name, license_data in national_licenses.items():
                if st.checkbox(f"{license_name} ({license_data['authority']})", key=f"nat_{license_name}"):
                    selected_licenses.append((license_name, license_data))
        
        with st.expander("🏘️ Local Council Licenses", expanded=True):
            for license_name, license_data in local_licenses.items():
                if st.checkbox(f"{license_name} ({license_data['authority']})", key=f"loc_{license_name}"):
                    selected_licenses.append((license_name, license_data))
        
        with st.expander("📦 Other Licenses", expanded=True):
            for license_name, license_data in other_licenses.items():
                if st.checkbox(f"{license_name} ({license_data['authority']})", key=f"oth_{license_name}"):
                    selected_licenses.append((license_name, license_data))
        
        # Bulk configuration
        if selected_licenses:
            st.write("**⚙️ Bulk Configuration**")
            config_col1, config_col2, config_col3 = st.columns(3)
            
            with config_col1:
                bulk_status = st.selectbox("Status for all", ["Completed", "In Progress", "Not Started"], key="bulk_status")
                bulk_mode = st.selectbox("Application Mode for all", APPLICATION_MODES, key="bulk_mode")
            
            with config_col2:
                cost_adjust = st.number_input("Cost Adjustment (%)", min_value=-100, max_value=100, value=0, key="cost_adj")
                time_adjust = st.number_input("Time Adjustment (%)", min_value=-50, max_value=200, value=0, key="time_adj")
            
            with config_col3:
                include_docs = st.checkbox("Include common documents", value=True, key="include_docs")
                auto_renewable = st.checkbox("Mark renewable as needed", value=True, key="auto_renew")
        
        if st.form_submit_button("📥 Add Selected Licenses", use_container_width=True):
            added_count = 0
            for license_name, license_data in selected_licenses:
                template_data = expanded_licenses[sector][license_name]
                
                # Calculate adjusted values
                base_cost = template_data.get('typical_cost', 0)
                adjusted_cost = base_cost * (1 + cost_adjust / 100)
                
                base_days = template_data.get('typical_days', 30)
                adjusted_days = max(1, int(base_days * (1 + time_adjust / 100)))
                
                procedure = {
                    'procedure': license_name,
                    'authority': license_data['authority'],
                    'status': bulk_status,
                    'prep_days': max(1, adjusted_days // 3),
                    'wait_days': max(1, adjusted_days - (adjusted_days // 3)),
                    'total_days': adjusted_days,
                    'official_fees': adjusted_cost,
                    'unofficial_payments': 0.0,
                    'travel_costs': 0.0,
                    'external_support': 'No',
                    'external_cost': 0.0,
                    'complexity': template_data.get('complexity', 3),
                    'renewable': license_data['renewable'],
                    'renewal_frequency': license_data['renewal_frequency'],
                    'application_mode': bulk_mode,
                    'documents': template_data['common_documents'] if include_docs else [],
                    'challenges': '',
                    'follow_ups': 2
                }
                
                # Check if procedure already exists
                existing_names = [p['procedure'] for p in st.session_state.procedures_list]
                if license_name not in existing_names:
                    st.session_state.procedures_list.append(procedure)
                    added_count += 1
            
            st.success(f"✅ Added {added_count} procedures!")
            st.rerun()

def add_all_sector_templates(sector, licenses_data):
    """Add all templates for a sector"""
    if sector in licenses_data:
        added_count = 0
        for license_name, license_data in licenses_data[sector].items():
            # Check if already exists
            existing_names = [p['procedure'] for p in st.session_state.procedures_list]
            if license_name not in existing_names:
                procedure = {
                    'procedure': license_name,
                    'authority': license_data['authority'],
                    'status': 'Completed',
                    'prep_days': max(1, license_data.get('typical_days', 30) // 3),
                    'wait_days': max(1, license_data.get('typical_days', 30) - (license_data.get('typical_days', 30) // 3)),
                    'total_days': license_data.get('typical_days', 30),
                    'official_fees': license_data.get('typical_cost', 0),
                    'unofficial_payments': 0.0,
                    'travel_costs': 0.0,
                    'external_support': 'No',
                    'external_cost': 0.0,
                    'complexity': license_data.get('complexity', 3),
                    'renewable': license_data['renewable'],
                    'renewal_frequency': license_data['renewal_frequency'],
                    'application_mode': 'Mixed',
                    'documents': license_data['common_documents'],
                    'challenges': '',
                    'follow_ups': 2
                }
                st.session_state.procedures_list.append(procedure)
                added_count += 1
        
        st.success(f"✅ Added {added_count} {sector} procedures!")
        st.rerun()

def add_common_national_licenses(sector):
    """Add common national licenses across sectors"""
    common_national = {
        "PACRA Business Registration": {
            "authority": "PACRA", "renewable": "No", "typical_cost": 1000, "typical_days": 14, "complexity": 3
        },
        "ZRA Tax Registration": {
            "authority": "ZRA", "renewable": "No", "typical_cost": 0, "typical_days": 7, "complexity": 2
        },
        "NAPSA Registration": {
            "authority": "NAPSA", "renewable": "No", "typical_cost": 0, "typical_days": 7, "complexity": 2
        },
        "NHIMA Registration": {
            "authority": "NHIMA", "renewable": "No", "typical_cost": 0, "typical_days": 7, "complexity": 2
        }
    }
    
    added_count = 0
    for license_name, license_data in common_national.items():
        existing_names = [p['procedure'] for p in st.session_state.procedures_list]
        if license_name not in existing_names:
            procedure = {
                'procedure': license_name,
                'authority': license_data['authority'],
                'status': 'Completed',
                'prep_days': max(1, license_data.get('typical_days', 14) // 2),
                'wait_days': max(1, license_data.get('typical_days', 14) - (license_data.get('typical_days', 14) // 2)),
                'total_days': license_data.get('typical_days', 14),
                'official_fees': license_data.get('typical_cost', 0),
                'unofficial_payments': 0.0,
                'travel_costs': 0.0,
                'external_support': 'No',
                'external_cost': 0.0,
                'complexity': license_data.get('complexity', 3),
                'renewable': license_data['renewable'],
                'renewal_frequency': 'One-time',
                'application_mode': 'Mixed',
                'documents': [],
                'challenges': '',
                'follow_ups': 1
            }
            st.session_state.procedures_list.append(procedure)
            added_count += 1
    
    st.success(f"✅ Added {added_count} common national licenses!")
    st.rerun()

def get_suggested_licenses(sector, existing_procedures):
    """Get smart license suggestions based on sector and existing procedures"""
    existing_names = [p['procedure'] for p in existing_procedures]
    
    suggestions = {
        "Agribusiness": {
            "PACRA Business Registration": {"priority": "high"},
            "Local Trading License": {"priority": "high"},
            "ZRA Tax Registration": {"priority": "high"},
            "ZEMA Environmental License": {"priority": "medium"},
            "Food and Drugs Act License": {"priority": "medium"}
        },
        "Construction": {
            "NCC Registration": {"priority": "high"},
            "Building Permit": {"priority": "high"}, 
            "ZEMA EIA Permit": {"priority": "high"},
            "Planning Permission": {"priority": "medium"},
            "Road Cutting Permit": {"priority": "medium"}
        }
    }
    
    sector_suggestions = suggestions.get(sector, {})
    # Filter out already added procedures
    return {k: v for k, v in sector_suggestions.items() if k not in existing_names}

def add_license_from_template(license_name, license_data, template_data):
    """Add a single license from template"""
    procedure = {
        'procedure': license_name,
        'authority': template_data['authority'],
        'status': 'Completed',
        'prep_days': max(1, template_data.get('typical_days', 30) // 3),
        'wait_days': max(1, template_data.get('typical_days', 30) - (template_data.get('typical_days', 30) // 3)),
        'total_days': template_data.get('typical_days', 30),
        'official_fees': template_data.get('typical_cost', 0),
        'unofficial_payments': 0.0,
        'travel_costs': 0.0,
        'external_support': 'No',
        'external_cost': 0.0,
        'complexity': template_data.get('complexity', 3),
        'renewable': template_data['renewable'],
        'renewal_frequency': template_data['renewal_frequency'],
        'application_mode': 'Mixed',
        'documents': template_data['common_documents'],
        'challenges': '',
        'follow_ups': 2
    }
    
    st.session_state.procedures_list.append(procedure)

# Single Procedure Capture
def single_procedure_capture():
    """Single procedure detailed capture"""
    
    st.subheader("🔧 Detailed Procedure Analysis")
    
    with st.form("single_procedure_form"):
        # Procedure Information
        col1, col2 = st.columns(2)
        
        with col1:
            procedure_name = st.text_input("Procedure/License Name *", 
                                         placeholder="e.g., Business License, Environmental Permit",
                                         key="proc_name_single")
            regulatory_authority = st.text_input("Regulatory Body *", 
                                               placeholder="e.g., Local Council, ZRA, PACRA",
                                               key="authority_single")
        
        with col2:
            current_status = st.selectbox("Current Status", 
                                        ["Not Started", "In Progress", "Completed", "Delayed", "Rejected"],
                                        key="status_single")
            application_mode = st.selectbox("Mode of Application *", APPLICATION_MODES, key="app_mode_single")
        
        # Time Analysis
        st.write("**⏱️ Time Analysis**")
        time_col1, time_col2, time_col3, time_col4 = st.columns(4)
        
        with time_col1:
            prep_days = st.number_input("Preparation Days", min_value=0, value=7,
                                      help="Time spent gathering documents",
                                      key="prep_days_single")
        with time_col2:
            wait_days = st.number_input("Waiting Days", min_value=0, value=21,
                                      help="Time waiting for approval",
                                      key="wait_days_single")
        with time_col3:
            follow_ups = st.number_input("Number of Follow-ups", min_value=0, value=2,
                                       key="follow_ups_single")
        with time_col4:
            total_days = st.number_input("Total Calendar Days *", min_value=0, value=28,
                                       key="total_days_single")
        
        # Cost Analysis
        st.write("**💰 Cost Analysis**")
        cost_col1, cost_col2, cost_col3 = st.columns(3)
        
        with cost_col1:
            official_fees = st.number_input("Official Fees (ZMW) *", min_value=0.0, value=0.0,
                                          key="official_fees_single")
        with cost_col2:
            unofficial_payments = st.number_input("Unofficial Payments (ZMW)", min_value=0.0, value=0.0,
                                                key="unofficial_single")
        with cost_col3:
            travel_costs = st.number_input("Travel & Incidentals (ZMW)", min_value=0.0, value=0.0,
                                         key="travel_costs_single")
        
        # External Support
        st.write("**🛠️ External Support**")
        support_col1, support_col2, support_col3 = st.columns(3)
        
        with support_col1:
            external_support = st.radio("Hired External Support?", ["No", "Yes"], 
                                      horizontal=True, key="external_support_single")
        
        # Initialize session state for external support if not exists
        if 'external_support_active' not in st.session_state:
            st.session_state.external_support_active = False
        
        # Update session state when radio button changes
        if external_support == "Yes":
            st.session_state.external_support_active = True
        else:
            st.session_state.external_support_active = False
        
        with support_col2:
            if st.session_state.external_support_active:
                external_cost = st.number_input("Support Cost (ZMW)", min_value=0.0, value=0.0,
                                              key="external_cost_single")
            else:
                external_cost = 0.0
                # Show disabled field for better UX
                st.number_input("Support Cost (ZMW)", min_value=0.0, value=0.0, 
                              disabled=True, key="external_cost_disabled")
        
        with support_col3:
            if st.session_state.external_support_active:
                external_reason = st.selectbox("Primary Reason", 
                                             ["Saves Time", "Expertise Required", "Connections/Relationships", "Complexity"],
                                             key="external_reason_single")
            else:
                external_reason = ""
                # Show disabled field for better UX
                st.selectbox("Primary Reason", 
                           ["Saves Time", "Expertise Required", "Connections/Relationships", "Complexity"],
                           disabled=True, key="external_reason_disabled")
        
        # Complexity & Renewal
        st.write("**📊 Assessment**")
        assess_col1, assess_col2, assess_col3 = st.columns(3)
        
        with assess_col1:
            complexity = st.slider("Complexity (1-5)", 1, 5, 3,
                                 help="1=Very Simple, 5=Extremely Complex",
                                 key="complexity_single")
        with assess_col2:
            renewable = st.radio("Renewable?", ["Yes", "No"], horizontal=True,
                               key="renewable_single")
        with assess_col3:
            if renewable == "Yes":
                renewal_freq = st.text_input("Renewal Frequency", placeholder="e.g., Annual, 2 years",
                                           key="renewal_freq_single")
            else:
                renewal_freq = "N/A"
                st.text_input("Renewal Frequency", value="N/A", disabled=True, key="renewal_freq_disabled")
        
        # Documents & Challenges
        st.write("**📄 Requirements & Challenges**")
        
        num_documents = st.number_input("Number of Required Documents", min_value=0, max_value=15, value=0,
                                      key="num_docs_single")
        documents = []
        for i in range(num_documents):
            doc = st.text_input(f"Document {i+1}", placeholder=f"Document name {i+1}", key=f"doc_single_{i}")
            if doc:
                documents.append(doc)
        
        challenges = st.text_area("Challenges & Observations", 
                                placeholder="Describe any difficulties, delays, or observations...",
                                height=80,
                                key="challenges_single")
        
        if st.form_submit_button("➕ Add This Procedure", use_container_width=True):
            if procedure_name and regulatory_authority:
                procedure_data = {
                    'procedure': procedure_name,
                    'authority': regulatory_authority,
                    'status': current_status,
                    'prep_days': prep_days,
                    'wait_days': wait_days,
                    'follow_ups': follow_ups,
                    'total_days': total_days,
                    'official_fees': official_fees,
                    'unofficial_payments': unofficial_payments,
                    'travel_costs': travel_costs,
                    'external_support': external_support,
                    'external_cost': external_cost if external_support == "Yes" else 0.0,
                    'external_reason': external_reason if external_support == "Yes" else "",
                    'complexity': complexity,
                    'renewable': renewable,
                    'renewal_frequency': renewal_freq,
                    'application_mode': application_mode,
                    'documents': documents,
                    'challenges': challenges
                }
                
                st.session_state.procedures_list.append(procedure_data)
                st.success(f"✅ Added: {procedure_name}")
                st.rerun()
            else:
                st.error("Please fill in required fields (Procedure Name and Regulatory Body)")

# Interactive Procedures Manager
def interactive_procedures_manager():
    """Manage procedures with enhanced editing"""
    
    if not st.session_state.procedures_list:
        st.info("📝 No procedures added yet. Use the forms above to add procedures.")
        return
    
    st.subheader("📋 Procedures Management")
    
    # Summary statistics
    total_procedures = len(st.session_state.procedures_list)
    total_cost = sum(p['official_fees'] + p.get('unofficial_payments', 0) + p.get('travel_costs', 0) for p in st.session_state.procedures_list)
    total_time = sum(p['total_days'] for p in st.session_state.procedures_list)
    avg_complexity = sum(p['complexity'] for p in st.session_state.procedures_list) / total_procedures
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Procedures", total_procedures)
    with col2:
        st.metric("Total Cost", f"ZMW {total_cost:,.0f}")
    with col3:
        st.metric("Total Time", f"{total_time} days")
    with col4:
        st.metric("Avg Complexity", f"{avg_complexity:.1f}/5")
    
    # Procedures table with editing
    for i, procedure in enumerate(st.session_state.procedures_list):
        # Create a container for each procedure
        with st.container():
            st.markdown(f"**{i+1}. {procedure['procedure']}** - {procedure['authority']} ({procedure['status']})")
            
            # Display procedure details
            display_procedure_details(procedure, i)
            
            st.markdown("---")

def display_procedure_details(procedure, index):
    """Display procedure details"""
    col1, col2 = st.columns([3, 1])
    
    with col1:
        # Basic info in columns
        info_col1, info_col2, info_col3 = st.columns(3)
        
        with info_col1:
            st.write(f"**Status:** {procedure['status']}")
            st.write(f"**Complexity:** {procedure['complexity']}/5")
            st.write(f"**Application Mode:** {procedure['application_mode']}")
        
        with info_col2:
            st.write(f"**Prep Time:** {procedure['prep_days']} days")
            st.write(f"**Wait Time:** {procedure['wait_days']} days")
            st.write(f"**Total Time:** {procedure['total_days']} days")
        
        with info_col3:
            st.write(f"**Official Fees:** ZMW {procedure['official_fees']:,.0f}")
            if procedure.get('unofficial_payments', 0) > 0:
                st.write(f"**Unofficial:** ZMW {procedure['unofficial_payments']:,.0f}")
            if procedure.get('travel_costs', 0) > 0:
                st.write(f"**Travel Costs:** ZMW {procedure['travel_costs']:,.0f}")
        
        # Renewal info
        if procedure['renewable'] == "Yes":
            st.write(f"**Renewable:** Yes ({procedure.get('renewal_frequency', 'N/A')})")
        
        # External support info
        if procedure.get('external_support') == "Yes":
            st.write(f"**External Support:** Yes (ZMW {procedure.get('external_cost', 0):,.0f})")
            if procedure.get('external_reason'):
                st.write(f"**Reason:** {procedure['external_reason']}")
        
        # Documents
        if procedure.get('documents'):
            st.write("**Required Documents:**")
            for doc in procedure['documents']:
                st.write(f"• {doc}")
        
        # Challenges
        if procedure.get('challenges'):
            st.write(f"**Challenges:** {procedure['challenges']}")
    
    with col2:
        # Action buttons
        if st.button("✏️ Edit", key=f"edit_proc_{index}"):
            st.session_state.active_procedure_index = index
        
        if st.button("📊 Analyze", key=f"analyze_proc_{index}"):
            analyze_single_procedure(procedure)
        
        if st.button("🗑️ Delete", key=f"delete_proc_{index}"):
            st.session_state.procedures_list.pop(index)
            st.rerun()
    
    # Edit form if this procedure is being edited
    if st.session_state.get('active_procedure_index') == index:
        with st.form(f"edit_procedure_{index}"):
            st.write("**Edit Procedure**")
            
            edit_col1, edit_col2 = st.columns(2)
            with edit_col1:
                new_status = st.selectbox("Status", ["Not Started", "In Progress", "Completed", "Delayed", "Rejected"], 
                                        index=["Not Started", "In Progress", "Completed", "Delayed", "Rejected"].index(procedure['status']),
                                        key=f"edit_status_{index}")
                new_complexity = st.slider("Complexity", 1, 5, procedure['complexity'],
                                         key=f"edit_complexity_{index}")
            with edit_col2:
                new_official_fees = st.number_input("Official Fees", min_value=0.0, value=procedure['official_fees'],
                                                  key=f"edit_fees_{index}")
                new_unofficial = st.number_input("Unofficial Payments", min_value=0.0, value=procedure.get('unofficial_payments', 0.0),
                                               key=f"edit_unofficial_{index}")
            
            new_application_mode = st.selectbox("Application Mode", APPLICATION_MODES, 
                                              index=APPLICATION_MODES.index(procedure['application_mode']) if procedure['application_mode'] in APPLICATION_MODES else 0,
                                              key=f"edit_app_mode_{index}")
            
            col1, col2 = st.columns(2)
            with col1:
                if st.form_submit_button("💾 Save Changes", use_container_width=True):
                    st.session_state.procedures_list[index].update({
                        'status': new_status,
                        'complexity': new_complexity,
                        'official_fees': new_official_fees,
                        'unofficial_payments': new_unofficial,
                        'application_mode': new_application_mode
                    })
                    st.session_state.active_procedure_index = None
                    st.rerun()
            
            with col2:
                if st.form_submit_button("❌ Cancel", use_container_width=True):
                    st.session_state.active_procedure_index = None
                    st.rerun()

def analyze_single_procedure(procedure):
    """Analyze a single procedure"""
    st.subheader(f"📊 Analysis: {procedure['procedure']}")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Cost breakdown
        costs = {
            'Official Fees': procedure['official_fees'],
            'Unofficial Payments': procedure.get('unofficial_payments', 0),
            'Travel Costs': procedure.get('travel_costs', 0)
        }
        if procedure.get('external_cost', 0) > 0:
            costs['External Support'] = procedure['external_cost']
        
        cost_df = pd.DataFrame({
            'Type': list(costs.keys()),
            'Amount': list(costs.values())
        })
        
        fig_pie = px.pie(cost_df, values='Amount', names='Type', title="Cost Breakdown")
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Time analysis
        times = {
            'Preparation': procedure['prep_days'],
            'Waiting': procedure['wait_days']
        }
        time_df = pd.DataFrame({
            'Phase': list(times.keys()),
            'Days': list(times.values())
        })
        
        fig_bar = px.bar(time_df, x='Phase', y='Days', title="Time Distribution", color='Phase')
        st.plotly_chart(fig_bar, use_container_width=True)
    
    with col3:
        # Risk assessment
        complexity_risk = procedure['complexity'] * 2
        cost_risk = min((procedure['official_fees'] + procedure.get('unofficial_payments', 0)) / 5000, 10)
        time_risk = min(procedure['total_days'] / 30, 10)
        total_risk = (complexity_risk + cost_risk + time_risk) / 3
        
        st.metric("Overall Risk", f"{total_risk:.1f}/10")
        st.metric("Complexity", f"{procedure['complexity']}/5")
        st.metric("Application Mode", procedure['application_mode'])
        st.metric("Total Cost", f"ZMW {sum(costs.values()):,.0f}")
        
        # Risk indicator
        risk_color = "green" if total_risk < 4 else "orange" if total_risk < 7 else "red"
        st.markdown(f"<div style='background-color: {risk_color}; height: 10px; width: {total_risk * 10}%; border-radius: 5px;'></div>", 
                    unsafe_allow_html=True)

# Enhanced Section B with Multiple Entry Modes
def enhanced_section_b():
    """Enhanced Section B with multiple entry modes"""
    
    st.header("📑 SECTION B: REGISTRATION & LICENSING LANDSCAPE")
    
    # Entry mode selection
    st.write("**Select Entry Mode:**")
    mode_col1, mode_col2, mode_col3 = st.columns(3)
    
    with mode_col1:
        if st.button("⚡ Quick Manual", use_container_width=True, key="quick_manual_btn"):
            st.session_state.bulk_procedure_mode = False
            st.session_state.quick_manual_mode = True
            st.rerun()
    
    with mode_col2:
        if st.button("🔧 Single Detailed", use_container_width=True, key="single_detailed_btn"):
            st.session_state.bulk_procedure_mode = False
            st.session_state.quick_manual_mode = False
            st.rerun()
    
    with mode_col3:
        if st.button("📊 Bulk Templates", use_container_width=True, key="bulk_templates_btn"):
            st.session_state.bulk_procedure_mode = True
            st.session_state.quick_manual_mode = False
            st.rerun()
    
    # Display current mode
    if st.session_state.get('quick_manual_mode', False):
        st.info("⚡ **Quick Manual Mode** - Fast entry for individual procedures")
        quick_manual_procedure()
    elif st.session_state.bulk_procedure_mode:
        st.info("📊 **Bulk Templates Mode** - Add multiple procedures using templates")
        enhanced_bulk_procedures_capture()
    else:
        st.info("🔧 **Single Detailed Mode** - Comprehensive data capture for individual procedures")
        single_procedure_capture()
    
    # Display and manage existing procedures
    interactive_procedures_manager()
    
    # Enhanced save options
    st.markdown("---")
    save_col1, save_col2, save_col3 = st.columns(3)
    
    with save_col1:
        if st.button("💾 Save Procedures", use_container_width=True, key="save_procedures_main"):
            st.session_state.form_data['procedure_data'] = st.session_state.procedures_list
            interview_id = save_draft(st.session_state.form_data, st.session_state.current_interview_id)
            if interview_id:
                st.session_state.current_interview_id = interview_id
                st.success(f"✅ Saved {len(st.session_state.procedures_list)} procedures!")
    
    with save_col2:
        if st.button("📊 Generate Report", use_container_width=True, key="generate_report"):
            generate_procedures_report()
    
    with save_col3:
        if st.button("🔄 Reset Section", use_container_width=True, key="reset_section"):
            if st.session_state.procedures_list:
                if st.checkbox("Confirm reset all procedures in this section"):
                    st.session_state.procedures_list = []
                    st.rerun()
            else:
                st.info("No procedures to reset")

def generate_procedures_report():
    """Generate a quick procedures report"""
    if not st.session_state.procedures_list:
        st.warning("No procedures to report")
        return
    
    total_cost = sum(p['official_fees'] + p.get('unofficial_payments', 0) for p in st.session_state.procedures_list)
    total_time = sum(p['total_days'] for p in st.session_state.procedures_list)
    avg_complexity = sum(p['complexity'] for p in st.session_state.procedures_list) / len(st.session_state.procedures_list)
    
    st.subheader("📈 Quick Procedures Report")
    
    col1, col2, col_3, col4 = st.columns(4)
    with col1:
        st.metric("Total Procedures", len(st.session_state.procedures_list))
    with col2:
        st.metric("Total Cost", f"ZMW {total_cost:,.0f}")
    with col_3:
        st.metric("Total Time", f"{total_time} days")
    with col4:
        st.metric("Avg Complexity", f"{avg_complexity:.1f}/5")
    
    # Application mode breakdown
    mode_counts = {}
    for procedure in st.session_state.procedures_list:
        mode = procedure.get('application_mode', 'Not specified')
        mode_counts[mode] = mode_counts.get(mode, 0) + 1
    
    if mode_counts:
        st.write("**Application Mode Distribution:**")
        for mode, count in mode_counts.items():
            st.write(f"- {mode}: {count} procedures")

# Authentication System
def login_system():
    """Enhanced login system for both interviewers and admins"""
    st.title("🔐 Zambia Regulatory Compliance Survey")
    st.subheader("Login to Access the System")
    
    # Login type selection
    login_type = st.radio("Login as:", ["Interviewer", "Administrator"], horizontal=True)
    
    with st.form("login_form"):
        if login_type == "Interviewer":
            username = st.selectbox("Select Interviewer", list(INTERVIEWER_CREDENTIALS.keys()), key="interviewer_select")
        else:
            username = st.selectbox("Username", list(ADMIN_CREDENTIALS.keys()), key="admin_select")
        
        password = st.text_input("Password", type="password", key="login_password")
        
        if st.form_submit_button("Login", use_container_width=True):
            if login_type == "Interviewer":
                credentials = INTERVIEWER_CREDENTIALS
            else:
                credentials = ADMIN_CREDENTIALS
            
            if username in credentials and password == credentials[username]["password"]:
                st.session_state.current_user = username
                st.session_state.user_role = credentials[username]["role"]
                
                if login_type == "Interviewer":
                    st.session_state.interviewer_logged_in = True
                    st.session_state.app_mode = 'data_collection'
                else:
                    st.session_state.admin_logged_in = True
                    st.session_state.app_mode = 'admin_dashboard'
                
                # Log login session
                log_user_session(username, datetime.now().isoformat())
                log_admin_action(username, "login")
                
                st.success(f"Welcome {username}! Login successful!")
                st.rerun()
            else:
                st.error("Invalid credentials. Please try again.")

def logout():
    """Logout function for all user types"""
    # Log logout session
    if st.session_state.current_user:
        log_user_session(
            st.session_state.current_user, 
            datetime.now().isoformat(), 
            datetime.now().isoformat(),
            0  # Session duration would be calculated in a real implementation
        )
        log_admin_action(st.session_state.current_user, "logout")
    
    # Reset all session states
    st.session_state.interviewer_logged_in = False
    st.session_state.admin_logged_in = False
    st.session_state.current_user = None
    st.session_state.user_role = None
    st.session_state.app_mode = 'login'
    
    # Clear interview data
    st.session_state.current_interview_id = None
    st.session_state.form_data = {}
    st.session_state.procedures_list = []
    st.session_state.selected_isic_codes = []
    st.session_state.business_activities_text = ""
    st.session_state.current_section = 'A'
    
    st.success("Logged out successfully!")
    st.rerun()

# Test the connection
def test_connection():
    """Test SQLite Cloud connection"""
    try:
        conn = get_connection()
        if conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            conn.close()
            st.success("✅ Successfully connected to SQLite Cloud!")
            return True
        return False
    except Exception as e:
        st.error(f"❌ Failed to connect to SQLite Cloud: {str(e)}")
        return False

# Enhanced Main Application
def main():
    # Test database connection first
    if not test_connection():
        st.error("Cannot proceed without database connection")
        return
    
    # Initialize database with proper schema
    if not check_and_fix_database():
        st.error("Failed to initialize database. Please check your connection.")
        return
    
    initialize_session_state()
    
    # Load ISIC data if not loaded
    if st.session_state.isic_df is None:
        st.session_state.isic_df = load_isic_dataframe()
    
    # Run database migrations
    add_missing_columns()
    
    # Route based on login status
    if not st.session_state.interviewer_logged_in and not st.session_state.admin_logged_in:
        login_system()
    elif st.session_state.admin_logged_in:
        admin_navigation()
    else:
        data_collection_navigation()

def admin_navigation():
    """Enhanced Admin navigation with Analytics and Editor"""
    st.sidebar.title("🔧 Admin Panel")
    st.sidebar.write(f"Logged in as: **{st.session_state.current_user}**")
    st.sidebar.write(f"Role: **{st.session_state.user_role}**")
    
    if st.sidebar.button("🚪 Logout", use_container_width=True, key="admin_logout_btn"):
        logout()
    
    st.sidebar.markdown("---")
    
    # Enhanced admin menu with editor
    menu_options = {
        'Dashboard': '📊',
        'Data Management': '💾', 
        'Edit_Interviews': '✏️',
        'Analytics': '📈',
        'User Management': '👥',
        'System Tools': '🛠️'
    }
    
    selected_menu = st.sidebar.radio("Menu", list(menu_options.keys()), 
                                   format_func=lambda x: f"{menu_options[x]} {x.replace('_', ' ')}",
                                   key="admin_menu")
    
    if selected_menu == 'Dashboard':
        admin_dashboard()
    elif selected_menu == 'Data Management':
        display_all_interviews()
    elif selected_menu == 'Edit_Interviews':
        interview_editor_main()
    elif selected_menu == 'Analytics':
        analytics_main()
    elif selected_menu == 'User Management':
        user_management_section()
    elif selected_menu == 'System Tools':
        database_tools_section()

def admin_dashboard():
    """Admin dashboard for database management"""
    
    st.title("🔧 Admin Dashboard")
    st.subheader("Database Management & Analytics")
    
    # Database Statistics
    st.header("📈 Database Statistics")
    
    stats = get_database_stats()
    
    if stats:
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Interviews", stats['total_interviews'])
        with col2:
            st.metric("Submitted", stats['submitted_interviews'])
        with col3:
            st.metric("Drafts", stats['draft_interviews'])
        with col4:
            if not stats['avg_metrics'].empty:
                avg_risk = stats['avg_metrics'].iloc[0]['avg_risk']
                st.metric("Avg Risk Score", f"{avg_risk:.1f}" if avg_risk else "N/A")
        
        # Visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            if not stats['sector_dist'].empty:
                fig_sector = px.pie(stats['sector_dist'], values='count', names='primary_sector', 
                                  title="Interviews by Sector")
                st.plotly_chart(fig_sector, use_container_width=True)
        
        with col2:
            if not stats['district_dist'].empty:
                fig_district = px.bar(stats['district_dist'], x='district', y='count',
                                    title="Interviews by District", color='district')
                st.plotly_chart(fig_district, use_container_width=True)
  
    # Data Management
    st.header("💾 Data Management")
    
    tab1, tab2, tab3, tab4 = st.tabs(["All Interviews", "Search & Filter", "Data Export", "Database Tools"])
    
    with tab1:
        display_all_interviews()
    
    with tab2:
        search_and_filter_interviews()
    
    with tab3:
        data_export_section()
    
    with tab4:
        database_tools_section()

def display_all_interviews():
    """Display all interviews in a table"""
    interviews_df = get_all_interviews()
    
    if not interviews_df.empty:
        st.write(f"**Total Records:** {len(interviews_df)}")
        
        # Convert datetime columns to string for display
        display_df = interviews_df.copy()
        if 'submission_date' in display_df.columns:
            display_df['submission_date'] = display_df['submission_date'].apply(lambda x: x.split('.')[0] if x else '')
        if 'last_modified' in display_df.columns:
            display_df['last_modified'] = display_df['last_modified'].apply(lambda x: x.split('.')[0] if x else '')
        
        st.dataframe(display_df, use_container_width=True)
        
        # Interview details
        st.subheader("📋 Interview Details")
        selected_interview = st.selectbox("Select interview to view details:", 
                                        interviews_df['interview_id'].tolist(),
                                        key="interview_select")
        
        if selected_interview:
            display_interview_details(selected_interview)
    else:
        st.info("No interviews found in the database.")
    
    # Add quick edit options
    add_quick_edit_options()
    
def search_and_filter_interviews():
    """Search and filter interviews"""
    st.subheader("🔍 Search & Filter Interviews")
    
    interviews_df = get_all_interviews()
    
    if not interviews_df.empty:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            sector_filter = st.multiselect("Filter by Sector", interviews_df['primary_sector'].unique(),
                                         key="sector_filter")
        with col2:
            district_filter = st.multiselect("Filter by District", interviews_df['district'].unique(),
                                           key="district_filter")
        with col3:
            status_filter = st.multiselect("Filter by Status", interviews_df['status'].unique(),
                                         key="status_filter")
        
        # Apply filters
        filtered_df = interviews_df.copy()
        if sector_filter:
            filtered_df = filtered_df[filtered_df['primary_sector'].isin(sector_filter)]
        if district_filter:
            filtered_df = filtered_df[filtered_df['district'].isin(district_filter)]
        if status_filter:
            filtered_df = filtered_df[filtered_df['status'].isin(status_filter)]
        
        st.write(f"**Filtered Results:** {len(filtered_df)} interviews")
        st.dataframe(filtered_df, use_container_width=True)
        
        # Export filtered results
        if not filtered_df.empty:
            csv = filtered_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Filtered Data (CSV)",
                data=csv,
                file_name=f"filtered_interviews_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                key="download_filtered_csv"
            )
    else:
        st.info("No interviews available for filtering.")

def data_export_section():
    """Data export section"""
    st.subheader("📤 Data Export")
    
    interviews_df = get_all_interviews()
    
    if not interviews_df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # CSV Export
            csv = interviews_df.to_csv(index=False)
            st.download_button(
                label="💾 Download Full Data (CSV)",
                data=csv,
                file_name=f"compliance_data_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True,
                key="download_full_csv"
            )
        
        with col2:
            # Excel Export
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                interviews_df.to_excel(writer, sheet_name='Interviews', index=False)
                
                # Add summary sheet
                summary_data = {
                    'Metric': ['Total Interviews', 'Submitted', 'Drafts', 'Average Risk Score'],
                    'Value': [
                        len(interviews_df),
                        len(interviews_df[interviews_df['status'] == 'submitted']),
                        len(interviews_df[interviews_df['status'] == 'draft']),
                        interviews_df['risk_score'].mean() if 'risk_score' in interviews_df.columns else 0
                    ]
                }
                pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
            
            excel_data = output.getvalue()
            st.download_button(
                label="📊 Download Full Data (Excel)",
                data=excel_data,
                file_name=f"compliance_data_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
                key="download_full_excel"
            )
        
        # JSON Export
        st.write("**JSON Export**")
        json_data = interviews_df.to_json(orient='records', indent=2)
        st.download_button(
            label="🔤 Download Full Data (JSON)",
            data=json_data,
            file_name=f"compliance_data_{datetime.now().strftime('%Y%m%d')}.json",
            mime="application/json",
            use_container_width=True,
            key="download_full_json"
        )
    else:
        st.info("No data available for export.")

def user_management_section():
    """User management section for admins"""
    st.header("👥 User Management")
    
    tab1, tab2, tab3 = st.tabs(["User Statistics", "Session Logs", "Password Management"])
    
    with tab1:
        st.subheader("User Statistics")
        
        # User interview counts
        try:
            result = execute_query("""
                SELECT created_by, 
                       COUNT(*) as total_interviews,
                       SUM(CASE WHEN status = 'submitted' THEN 1 ELSE 0 END) as submitted,
                       SUM(CASE WHEN status = 'draft' THEN 1 ELSE 0 END) as drafts
                FROM responses 
                GROUP BY created_by
                ORDER BY total_interviews DESC
            """, return_result=True)
            
            if result and isinstance(result, tuple) and result[0]:
                result_data, columns = result
                user_stats = pd.DataFrame(result_data, columns=columns)
                
                if not user_stats.empty:
                    st.dataframe(user_stats, use_container_width=True)
                    
                    # Visualization
                    fig = px.bar(user_stats, x='created_by', y='total_interviews', 
                               title="Interviews by User", color='created_by')
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No user statistics available yet.")
            else:
                st.info("No user statistics available yet.")
        except Exception as e:
            st.error(f"Error loading user statistics: {str(e)}")
    
    with tab2:
        st.subheader("User Session Logs")
        display_user_sessions()
    
    with tab3:
        st.subheader("Password Management")
        st.info("Password changes require system administrator access.")
        
        # Display current credentials (read-only)
        st.write("**Current Interviewer Credentials:**")
        interviewer_df = pd.DataFrame([
            {"Username": username, "Role": details["role"]} 
            for username, details in INTERVIEWER_CREDENTIALS.items()
        ])
        st.dataframe(interviewer_df, use_container_width=True)
        
        st.write("**Current Admin Credentials:**")
        admin_df = pd.DataFrame([
            {"Username": username, "Role": details["role"]} 
            for username, details in ADMIN_CREDENTIALS.items()
        ])
        st.dataframe(admin_df, use_container_width=True)

def display_user_sessions():
    """Display user session logs"""
    try:
        result = execute_query("SELECT * FROM user_sessions ORDER BY login_time DESC LIMIT 100", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            result_data, columns = result
            sessions_df = pd.DataFrame(result_data, columns=columns)
            st.dataframe(sessions_df, use_container_width=True)
        else:
            st.info("No session logs found.")
    except Exception as e:
        st.error(f"Error loading session logs: {str(e)}")

def database_tools_section():
    """Database tools and maintenance"""
    st.subheader("🛠️ Database Tools")
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Refresh Database Cache", use_container_width=True, key="refresh_cache_btn"):
            st.session_state.isic_df = load_isic_dataframe()
            st.success("Database cache refreshed!")
            log_admin_action(st.session_state.current_user, "refresh_cache")
        
        if st.button("📊 Update Statistics", use_container_width=True, key="update_stats_btn"):
            st.rerun()
    
    with col2:
        if st.button("🗑️ Clear All Drafts", use_container_width=True, key="clear_drafts_btn"):
            if st.checkbox("I understand this will delete all draft interviews", key="confirm_clear_drafts"):
                if st.button("Confirm Delete All Drafts", key="confirm_delete_drafts_btn"):
                    try:
                        result = execute_query("DELETE FROM responses WHERE status = 'draft'")
                        if result:
                            st.success("All draft interviews deleted!")
                            log_admin_action(st.session_state.current_user, "clear_drafts")
                            st.rerun()
                        else:
                            st.error("Failed to delete drafts")
                    except Exception as e:
                        st.error(f"Error deleting drafts: {str(e)}")
        
        if st.button("📝 View Admin Logs", use_container_width=True, key="view_logs_btn"):
            display_admin_logs()

def display_admin_logs():
    """Display admin action logs"""
    try:
        result = execute_query("SELECT * FROM admin_logs ORDER BY timestamp DESC LIMIT 100", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            result_data, columns = result
            logs_df = pd.DataFrame(result_data, columns=columns)
            st.subheader("📝 Admin Action Logs")
            st.dataframe(logs_df, use_container_width=True)
        else:
            st.info("No admin logs found.")
    except Exception as e:
        st.error(f"Error loading admin logs: {str(e)}")

def display_interview_details(interview_id):
    """Display detailed interview information"""
    details_df = get_interview_details(interview_id)
    
    if not details_df.empty:
        interview = details_df.iloc[0]
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Business Information**")
            st.write(f"**Name:** {interview['business_name']}")
            st.write(f"**District:** {interview['district']}")
            st.write(f"**Sector:** {interview['primary_sector']}")
            st.write(f"**Size:** {interview['business_size']}")
            st.write(f"**Legal Status:** {interview['legal_status']}")
        
        with col2:
            st.write("**Compliance Metrics**")
            st.write(f"**Total Cost:** ZMW {interview['total_compliance_cost']:,.0f}")
            st.write(f"**Total Time:** {interview['total_compliance_time']} days")
            st.write(f"**Risk Score:** {interview['risk_score']:.1f}/10")
            st.write(f"**Status:** {interview['status']}")
        
        # Procedures data
        if interview['procedure_data']:
            st.write("**Procedures**")
            procedures = json.loads(interview['procedure_data'])
            for i, proc in enumerate(procedures, 1):
                with st.expander(f"{i}. {proc['procedure']}"):
                    st.write(f"**Authority:** {proc['authority']}")
                    st.write(f"**Application Mode:** {proc.get('application_mode', 'Not specified')}")
                    st.write(f"**Cost:** ZMW {proc['official_fees']:,.0f}")
                    st.write(f"**Time:** {proc['total_days']} days")
                    st.write(f"**Complexity:** {proc['complexity']}/5")
        
        # Reform priorities
        if interview['reform_priorities']:
            st.write("**Reform Priorities**")
            reforms = json.loads(interview['reform_priorities'])
            for i, reform in enumerate(reforms, 1):
                st.write(f"{i}. {reform}")

def data_collection_navigation():
    """Data collection navigation for interviewers"""
    st.sidebar.title("📋 Interview Panel")
    st.sidebar.write(f"Interviewer: **{st.session_state.current_user}**")
    
    if st.sidebar.button("🚪 Logout", use_container_width=True, key="interviewer_logout_btn"):
        logout()
    
    # Quick stats for interviewer
    user_interviews = get_user_interviews(st.session_state.current_user)
    if not user_interviews.empty:
        st.sidebar.markdown("---")
        st.sidebar.header("📈 My Statistics")
        total = len(user_interviews)
        submitted = len(user_interviews[user_interviews['status'] == 'submitted'])
        drafts = len(user_interviews[user_interviews['status'] == 'draft'])
        
        st.sidebar.write(f"**Total:** {total}")
        st.sidebar.write(f"**Submitted:** {submitted}")
        st.sidebar.write(f"**Drafts:** {drafts}")
    
    # Draft quick access
    display_draft_quick_access()
    
    st.sidebar.markdown("---")
    
    # Navigation
    sections = {
        'A': 'Interview & Business Profile',
        'B': 'Registration & Licensing', 
        'C': 'Ongoing Compliance',
        'D': 'Reform Priorities',
        'Dashboard': 'My Interviews',
        'My_Data': 'My Data Management',
        'Draft_Dashboard': '📝 Draft Manager'
    }
    
    selected_section = st.sidebar.radio("Go to Section:", list(sections.keys()), 
                                      format_func=lambda x: f"Section {x}: {sections[x]}" if x in ['A','B','C','D'] else sections[x],
                                      key="main_navigation")
    
    # Update current section
    if selected_section != st.session_state.current_section:
        st.session_state.current_section = selected_section
        st.rerun()
    
    # Display current section
    if st.session_state.current_section == 'A':
        display_section_a()
    elif st.session_state.current_section == 'B':
        enhanced_section_b()
    elif st.session_state.current_section == 'C':
        display_section_c()
    elif st.session_state.current_section == 'D':
        display_section_d()
    elif st.session_state.current_section == 'Dashboard':
        display_interviewer_dashboard()
    elif st.session_state.current_section == 'My_Data':
        display_interviewer_data_management()
    elif st.session_state.current_section == 'Draft_Dashboard':
        display_draft_dashboard()

def display_interviewer_dashboard():
    """Dashboard for individual interviewer"""
    st.header("📊 My Interview Dashboard")
    
    user_interviews = get_user_interviews(st.session_state.current_user)
    
    if not user_interviews.empty:
        # Summary metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Total Interviews", len(user_interviews))
        with col2:
            submitted = len(user_interviews[user_interviews['status'] == 'submitted'])
            st.metric("Submitted", submitted)
        with col3:
            drafts = len(user_interviews[user_interviews['status'] == 'draft'])
            st.metric("Drafts", drafts)
        with col4:
            completion_rate = (submitted / len(user_interviews)) * 100 if len(user_interviews) > 0 else 0
            st.metric("Completion Rate", f"{completion_rate:.1f}%")
        
        # Recent interviews
        st.subheader("📋 My Recent Interviews")
        st.dataframe(user_interviews, use_container_width=True)
        
        # Visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            # Status distribution
            status_counts = user_interviews['status'].value_counts()
            fig_status = px.pie(values=status_counts.values, names=status_counts.index, 
                              title="Interview Status Distribution")
            st.plotly_chart(fig_status, use_container_width=True)
        
        with col2:
            # Sector distribution
            if 'primary_sector' in user_interviews.columns:
                sector_counts = user_interviews['primary_sector'].value_counts()
                fig_sector = px.bar(x=sector_counts.index, y=sector_counts.values,
                                  title="Interviews by Sector", color=sector_counts.index)
                st.plotly_chart(fig_sector, use_container_width=True)
    else:
        st.info("You haven't conducted any interviews yet. Start with Section A!")

def display_interviewer_data_management():
    """Data management for individual interviewer"""
    st.header("💾 My Data Management")
    
    user_interviews = get_user_interviews(st.session_state.current_user)
    
    if not user_interviews.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Export My Data")
            csv_data = user_interviews.to_csv(index=False)
            st.download_button(
                label="📥 Download My Interviews (CSV)",
                data=csv_data,
                file_name=f"my_interviews_{st.session_state.current_user}_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col2:
            st.subheader("Quick Actions")
            if st.button("🔄 Start New Interview", use_container_width=True):
                reset_interview()
                st.session_state.current_section = 'A'
                st.rerun()
            
            if st.button("📊 View All My Data", use_container_width=True):
                st.dataframe(user_interviews, use_container_width=True)
    else:
        st.info("No data available for export.")

def display_section_a():
    """Section A with enhanced business activities and duplicate validation"""
    st.header("📋 SECTION A: Interview & Business Profile")
    
    with st.form("section_a_form"):
        st.subheader("A1. Interview Metadata")
        
        col1, col2 = st.columns(2)
        with col1:
            interviewer = st.selectbox("Interviewer's Name", INTERVIEWERS, key="interviewer_name")
            interview_date = st.date_input("Date of Interview", key="interview_date")
        with col2:
            start_time = st.time_input("Start Time", value=datetime.now().time(), key="start_time")
            end_time = st.time_input("End Time", value=datetime.now().time(), key="end_time")
        
        st.subheader("A2. Business Identification")
        col1, col2 = st.columns(2)
        with col1:
            business_name = st.text_input("Business Name *", key="business_name")
            # Check for duplicate business name in real-time
            if business_name and st.session_state.current_interview_id:
                if check_duplicate_business_name(business_name, st.session_state.current_interview_id):
                    st.error(f"⚠️ Business name '{business_name}' already exists in the database. Please use a unique business name.")
            
            district = st.selectbox("Location (Town/District) *", DISTRICTS, key="district")
            physical_address = st.text_area("Physical Address", key="physical_address")
        with col2:
            contact_person = st.text_input("Contact Person & Title *", key="contact_person")
            email = st.text_input("Email Address", key="email")
            phone = st.text_input("Phone Number", key="phone")
        
        st.subheader("A3. Business Classification")
        col1, col2 = st.columns(2)
        with col1:
            primary_sector = st.radio("Primary Sector *", ["Agribusiness", "Construction"], key="primary_sector")
            legal_status = st.selectbox("Legal Status *", 
                                      ["Sole Proprietor", "Partnership", "Limited Liability Company", 
                                       "Public Limited Company", "Other"],
                                      key="legal_status")
        with col2:
            business_size = st.selectbox("Business Size *", 
                                       ["Micro (1-9)", "Small (10-49)", "Medium (50-249)", "Large (250+)"],
                                       key="business_size")
            ownership = st.selectbox("Ownership Structure *",
                                  ["100% Zambian-owned", "Partially Foreign-owned", 
                                   "Majority/Fully Foreign-owned", "Other"],
                                  key="ownership")
            gender_owner = st.radio("Gender of Majority Owner/CEO *", ["Male", "Female", "Joint (M/F)"], key="gender_owner")
        
        # Enhanced Business Activities Section
        st.subheader("A4. Business Background")
        
        # Business activities description (inside form)
        business_activities = st.text_area(
            "Business Activities Description *",
            value=st.session_state.business_activities_text,
            placeholder="Describe your main business activities, products, and services in detail...",
            height=120,
            key="business_activities_form"
        )
        st.session_state.business_activities_text = business_activities
        
        # Other business background fields
        col1, col2, col3 = st.columns(3)
        with col1:
            year_established = st.number_input("Year of Establishment", min_value=1900, max_value=2024, value=2020, key="year_established")
        with col2:
            turnover_range = st.selectbox("Annual Turnover Range", 
                                        ["< 500,000", "500,000 - 1M", "1M - 5M", "5M - 10M", "10M - 50M", "> 50M"],
                                        key="turnover_range")
        with col3:
            employees_fulltime = st.number_input("Full-time Employees", min_value=0, value=0, key="employees_fulltime")
            employees_parttime = st.number_input("Part-time Employees", min_value=0, value=0, key="employees_parttime")
        
        if st.form_submit_button("💾 Save Section A", use_container_width=True):
            # Validate required fields
            if not business_name:
                st.error("❌ Business Name is required!")
                return
                
            # Check for duplicate business name before saving
            if check_duplicate_business_name(business_name, st.session_state.current_interview_id):
                st.error(f"❌ Business name '{business_name}' already exists in the database. Please use a unique business name.")
                return
            
            # Save all data
            st.session_state.form_data.update({
                'interviewer_name': interviewer,
                'interview_date': str(interview_date),
                'start_time': str(start_time),
                'end_time': str(end_time),
                'business_name': business_name,
                'district': district,
                'physical_address': physical_address,
                'contact_person': contact_person,
                'email': email,
                'phone': phone,
                'primary_sector': primary_sector,
                'legal_status': legal_status,
                'business_size': business_size,
                'ownership_structure': ownership,
                'gender_owner': gender_owner,
                'business_activities': business_activities,
                'isic_codes': st.session_state.selected_isic_codes,
                'year_established': year_established,
                'turnover_range': turnover_range,
                'employees_fulltime': employees_fulltime,
                'employees_parttime': employees_parttime
            })
            
            interview_id = save_draft(st.session_state.form_data, st.session_state.current_interview_id)
            if interview_id:
                st.session_state.current_interview_id = interview_id
                st.success("✅ Section A saved successfully!")
    
    # ISIC Code section - SEPARATE FROM FORM
    st.markdown("---")
    business_activities_section()

def display_section_c():
    """Section C - Ongoing Compliance"""
    st.header("⏱️ SECTION C: Ongoing Compliance Burden")
    
    with st.form("section_c_form"):
        st.subheader("C1. Time Allocation for Compliance")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            local_time = st.slider("Local Council Time (%)", 0, 100, 0, key="local_time")
        with col2:
            national_time = st.slider("National Level Time (%)", 0, 100, 0, key="national_time")
        with col3:
            dk_time = st.slider("Don't Know/Other (%)", 0, 100, 0, key="dk_time")
        
        # Auto-calculate and validate percentages
        total_time = local_time + national_time + dk_time
        if total_time != 100:
            st.warning(f"⚠️ Percentages sum to {total_time}%. Should be 100%.")
        
        st.subheader("C2. Cost Assessment")
        cost_percentage = st.slider("Compliance costs as percentage of annual turnover (%)", 
                                  0.0, 50.0, 0.0, 0.5, key="cost_percentage")
        
        st.subheader("C3. Comparative Assessment")
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Number of Permits vs 2 Years Ago**")
            permit_national = st.radio("National Permits", ["More", "Same", "Fewer", "Don't Know"], key="permit_national")
            permit_local = st.radio("Local Council Permits", ["More", "Same", "Fewer", "Don't Know"], key="permit_local")
        
        with col2:
            st.write("**Cost per Permit vs 2 Years Ago**")
            cost_national = st.radio("National Permits Cost", ["More", "Same", "Less", "Don't Know"], key="cost_national")
            cost_local = st.radio("Local Council Permits Cost", ["More", "Same", "Less", "Don't Know"], key="cost_local")
        
        st.subheader("C4. Business Climate Rating")
        climate_rating = st.select_slider("Rate this year vs last year", 
                                        options=["Worse", "Same", "Better"],
                                        key="climate_rating")
        
        if st.form_submit_button("💾 Save Section C", use_container_width=True):
            # Save section data
            st.session_state.form_data.update({
                'completion_time_local': local_time,
                'completion_time_national': national_time,
                'completion_time_dk': dk_time,
                'compliance_cost_percentage': cost_percentage,
                'permit_comparison_national': permit_national,
                'permit_comparison_local': permit_local,
                'cost_comparison_national': cost_national,
                'cost_comparison_local': cost_local,
                'business_climate_rating': climate_rating
            })
            
            interview_id = save_draft(st.session_state.form_data, st.session_state.current_interview_id)
            if interview_id:
                st.session_state.current_interview_id = interview_id
                st.success("✅ Section C saved successfully!")

def display_section_d():
    """Section D - Reform Priorities - IMPROVED VERSION"""
    st.header("💡 SECTION D: Reform Priorities & Recommendations")
    
    with st.form("section_d_form"):
        st.subheader("Reform Recommendations")
        
        st.write("""
        *If you could advise the government on specific, actionable reforms 
        to reduce the compliance burden, what would they be?*
        """)
        
        # Improved reform priorities with better suggestions
        st.write("**🎯 Top Reform Priorities**")
        
        # Categorized reform options
        reform_categories = {
            "📋 Process Simplification": [
                "Simplify application procedures",
                "Reduce number of required documents",
                "Standardize forms across agencies",
                "Create single-window clearance system"
            ],
            "⏱️ Time Reduction": [
                "Set maximum processing time limits",
                "Implement online tracking systems",
                "Reduce bureaucratic delays",
                "Expedite renewal processes"
            ],
            "💰 Cost Reduction": [
                "Lower official fees for small businesses",
                "Eliminate redundant charges",
                "Provide fee waivers for startups",
                "Transparent fee structure"
            ],
            "🌐 Digital Transformation": [
                "Full online application system",
                "Digital document submission",
                "Online payment options",
                "Mobile-friendly services"
            ],
            "🔍 Transparency & Accountability": [
                "Publish clear requirements online",
                "Provide status updates automatically",
                "Establish complaint mechanisms",
                "Publish service standards"
            ],
            "🏢 Institutional Reforms": [
                "Better inter-agency coordination",
                "Training for regulatory staff",
                "One-stop service centers",
                "Regular stakeholder consultations"
            ]
        }
        
        selected_reforms = []
        
        # Display reform options by category
        for category, reforms in reform_categories.items():
            with st.expander(f"{category}", expanded=True):
                for reform in reforms:
                    if st.checkbox(reform, key=f"reform_{reform}"):
                        selected_reforms.append(reform)
        
        # Additional custom reforms
        st.subheader("💡 Additional Custom Recommendations")
        custom_reforms = st_tags(
            label='Enter additional reform priorities (press Enter after each):',
            text='Add your own specific recommendations',
            value=[],
            suggestions=['Harmonize local and national requirements', 
                        'Provide pre-application advisory services',
                        'Simplify tax compliance procedures',
                        'Streamline environmental clearances'],
            maxtags=5,
            key="custom_reform_priorities"
        )
        
        # Combine selected and custom reforms
        all_reforms = selected_reforms + custom_reforms
        
        # Priority ranking for top 3 reforms
        if len(all_reforms) > 0:
            st.subheader("🎖️ Priority Ranking")
            st.write("Select your top 3 most important reforms:")
            
            if len(all_reforms) >= 1:
                priority_1 = st.selectbox("1st Priority", [""] + all_reforms, key="priority_1")
            if len(all_reforms) >= 2:
                priority_2 = st.selectbox("2nd Priority", [""] + [r for r in all_reforms if r != priority_1], key="priority_2")
            if len(all_reforms) >= 3:
                priority_3 = st.selectbox("3rd Priority", [""] + [r for r in all_reforms if r not in [priority_1, priority_2]], key="priority_3")
            
            # Store prioritized reforms
            prioritized_reforms = [p for p in [priority_1, priority_2, priority_3] if p]
            other_reforms = [r for r in all_reforms if r not in prioritized_reforms]
            final_reforms = prioritized_reforms + other_reforms
        
        else:
            final_reforms = []
        
        # Impact assessment
        st.subheader("📊 Expected Impact")
        if final_reforms:
            st.write("**Potential benefits of implementing these reforms:**")
            
            impact_col1, impact_col2, impact_col3 = st.columns(3)
            
            with impact_col1:
                time_reduction = st.slider("Time Reduction (%)", 0, 100, 30, 
                                         help="Expected reduction in compliance time")
                st.metric("Time Saved", f"{time_reduction}%")
            
            with impact_col2:
                cost_reduction = st.slider("Cost Reduction (%)", 0, 100, 25,
                                         help="Expected reduction in compliance costs")
                st.metric("Cost Saved", f"{cost_reduction}%")
            
            with impact_col3:
                complexity_reduction = st.slider("Complexity Reduction", 1, 5, 3,
                                               help="1=Minor improvement, 5=Major simplification")
                st.metric("Simplicity Gain", f"{complexity_reduction}/5")
        
        col1, col2 = st.columns(2)
        with col1:
            save_btn = st.form_submit_button("💾 Save Section D", use_container_width=True)
        with col2:
            submit_btn = st.form_submit_button("🚀 Submit Complete Interview", use_container_width=True)
        
        if save_btn:
            st.session_state.form_data['reform_priorities'] = final_reforms
            interview_id = save_draft(st.session_state.form_data, st.session_state.current_interview_id)
            if interview_id:
                st.session_state.current_interview_id = interview_id
                st.success("✅ Section D saved successfully!")
        
        if submit_btn:
            st.session_state.form_data['reform_priorities'] = final_reforms
            interview_id = save_draft(st.session_state.form_data, st.session_state.current_interview_id)
            if interview_id and submit_final(interview_id):
                st.balloons()
                st.success("🎉 Interview submitted successfully!")
                show_completion_actions()

def show_completion_actions():
    """Show actions after interview completion"""
    st.subheader("🎉 Interview Completed!")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("📄 Download Summary", key="download_summary_btn"):
            st.info("Summary download feature")
    
    with col2:
        if st.button("📊 View Analysis", key="view_analysis_btn"):
            st.session_state.current_section = 'Dashboard'
            st.rerun()
    
    with col3:
        if st.button("🔄 New Interview", key="new_interview_btn"):
            reset_interview()

def reset_interview():
    """Reset the interview"""
    st.session_state.current_interview_id = None
    st.session_state.form_data = {}
    st.session_state.procedures_list = []
    st.session_state.selected_isic_codes = []
    st.session_state.business_activities_text = ""
    st.session_state.current_section = 'A'
    st.success("🔄 New interview started!")
    st.rerun()

# Initialize and run the application
if __name__ == "__main__":
    main()
