# app.py - Zambia Regulatory Compliance Survey with SQLite Cloud and pyisic integration
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, time
import json
import os
import plotly.express as px
import plotly.graph_objects as go
import io
import base64
import hashlib
import sqlitecloud
from typing import List, Dict, Any, Optional

# Import pyisic for ISIC classification
try:
    import pyisic
    from pyisic import ISICVersion, ISICType
    PYISIC_AVAILABLE = True
except ImportError:
    PYISIC_AVAILABLE = False
    st.warning("⚠️ pyisic not available. Using fallback ISIC data.")

# Import modules
try:
    from interview_editor import interview_editor_main
except ImportError:
    def interview_editor_main():
        st.info("📝 Interview Editor - Module not available")

try:
    from analytics_dashboard import analytics_main, ComplianceAnalytics
except ImportError:
    def analytics_main():
        st.info("📊 Analytics Dashboard - Module not available")
    
    class ComplianceAnalytics:
        def __init__(self):
            pass

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

# Enhanced credentials
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

# Application modes
APPLICATION_MODES = ["Entirely In-Person", "Mixed", "Entirely Online"]
DISTRICTS = ["Lusaka", "Kitwe", "Kasama", "Ndola", "Livingstone", "Other (Please specify)"]
INTERVIEWERS = list(INTERVIEWER_CREDENTIALS.keys())

# Initialize session state variables
def initialize_session_state():
    """Initialize all session state variables"""
    defaults = {
        'logged_in': False,
        'current_user': None,
        'user_role': None,
        'interviewer_logged_in': False,
        'admin_logged_in': False,
        'current_section': 'A',
        'current_interview_id': None,
        'form_data': {},
        'procedures_list': [],
        'selected_isic_codes': [],
        'business_activities_text': "",
        'show_draft_dashboard': False,
        'custom_procedures': [],
        'custom_authorities': [],
        'manual_isic_input': "",
        'selected_isic_for_business': "",
        'isic_search_term': "",
        'show_detailed_form': False,
        'use_template': False,
        'interview_start_time': None,
        'active_procedure_index': None,
        'district_specific_notes': {},
        'isic_df': None,
        'bulk_procedure_mode': False,
        'quick_manual_mode': False,
        'app_mode': 'login',
        'database_initialized': False
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

# Initialize the session state
initialize_session_state()

# Draft Manager Class
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
                return pd.DataFrame(result_data, columns=columns)
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
                return pd.DataFrame(result_data, columns=columns)
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
                df = pd.DataFrame(result_data, columns=columns)
                if not df.empty:
                    return df.iloc[0].to_dict()
            return None
        except Exception as e:
            st.error(f"Error loading draft: {str(e)}")
            return None
    
    def update_draft_progress(self, interview_id, current_section, progress_percentage):
        """Update draft progress and current section"""
        try:
            current_time = datetime.now().isoformat()
            result = execute_query('''
                UPDATE responses 
                SET current_section = ?, draft_progress = ?, last_modified = ?
                WHERE interview_id = ?
            ''', (current_section, progress_percentage, current_time, interview_id))
            return result is not None
        except Exception as e:
            st.error(f"Error updating draft progress: {str(e)}")
            return False
    
    def delete_draft(self, interview_id):
        """Delete a draft interview"""
        try:
            result = execute_query("DELETE FROM responses WHERE interview_id = ? AND status = 'draft'", (interview_id,))
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

def reset_interview():
    """Reset the interview session state"""
    st.session_state.current_interview_id = None
    st.session_state.form_data = {}
    st.session_state.procedures_list = []
    st.session_state.selected_isic_codes = []
    st.session_state.business_activities_text = ""
    st.session_state.current_section = 'A'

# pyisic Integration for ISIC Classification
class PyISICClient:
    """Client for ISIC classification using pyisic library"""
    
    def __init__(self):
        self.available = PYISIC_AVAILABLE
        if self.available:
            st.success("✅ pyisic library loaded successfully!")
        else:
            st.warning("⚠️ pyisic not available - using fallback data")
    
    def search_isic_codes(self, search_term: str, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Search ISIC codes using pyisic library
        
        Args:
            search_term: Term to search for (code or description)
            limit: Maximum number of results to return
            
        Returns:
            List of ISIC code dictionaries
        """
        if not self.available:
            return self._fallback_search(search_term, limit)
        
        try:
            results = []
            search_term_lower = search_term.lower().strip()
            
            # Search through ISIC versions (try both ISIC 4 and 3.1)
            for version in [ISICVersion.ISIC4, ISICVersion.ISIC3_1]:
                try:
                    # Get all codes for this version
                    all_codes = pyisic.get_codes(version)
                    
                    for code, details in all_codes.items():
                        # Check if search term matches code or description
                        code_match = search_term_lower in code.lower()
                        desc_match = search_term_lower in details.description.lower() if details.description else False
                        
                        if code_match or desc_match:
                            results.append({
                                'code': code,
                                'title': details.description,
                                'description': details.description,
                                'display': f"{code} - {details.description}",
                                'full_info': f"ISIC {code}: {details.description}",
                                'version': version.value,
                                'level': getattr(details, 'level', 'N/A')
                            })
                            
                            # Break if we have enough results
                            if len(results) >= limit:
                                break
                
                except Exception as e:
                    continue  # Try next version if one fails
            
            # Remove duplicates based on code
            seen_codes = set()
            unique_results = []
            for result in results:
                if result['code'] not in seen_codes:
                    seen_codes.add(result['code'])
                    unique_results.append(result)
            
            return unique_results[:limit]
            
        except Exception as e:
            st.error(f"❌ pyisic search error: {str(e)}")
            return self._fallback_search(search_term, limit)
    
    def get_isic_code_details(self, code: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information for a specific ISIC code
        
        Args:
            code: ISIC code to lookup
            
        Returns:
            Dictionary with code details or None if not found
        """
        if not self.available:
            return self._fallback_get_details(code)
        
        try:
            # Try different ISIC versions
            for version in [ISICVersion.ISIC4, ISICVersion.ISIC3_1]:
                try:
                    details = pyisic.get_code(code, version)
                    if details:
                        return {
                            'code': code,
                            'title': details.description,
                            'description': details.description,
                            'version': version.value,
                            'level': getattr(details, 'level', 'N/A'),
                            'parent': getattr(details, 'parent', None)
                        }
                except:
                    continue
            
            return None
            
        except Exception as e:
            return self._fallback_get_details(code)
    
    def validate_isic_code(self, code: str) -> bool:
        """
        Validate if a code is a valid ISIC code
        
        Args:
            code: ISIC code to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not self.available:
            return self._fallback_validate(code)
        
        try:
            for version in [ISICVersion.ISIC4, ISICVersion.ISIC3_1]:
                try:
                    details = pyisic.get_code(code, version)
                    if details:
                        return True
                except:
                    continue
            return False
        except:
            return self._fallback_validate(code)
    
    def _fallback_search(self, search_term: str, limit: int) -> List[Dict[str, Any]]:
        """Fallback search when pyisic is not available"""
        basic_data = self._get_fallback_data()
        search_term_lower = search_term.lower()
        
        results = []
        for code, title in basic_data.items():
            if (search_term_lower in code.lower() or 
                search_term_lower in title.lower()):
                results.append({
                    'code': code,
                    'title': title,
                    'description': title,
                    'display': f"{code} - {title}",
                    'full_info': f"ISIC {code}: {title}",
                    'version': 'ISIC4',
                    'level': 'N/A'
                })
                
                if len(results) >= limit:
                    break
        
        return results
    
    def _fallback_get_details(self, code: str) -> Optional[Dict[str, Any]]:
        """Fallback get details when pyisic is not available"""
        basic_data = self._get_fallback_data()
        if code in basic_data:
            return {
                'code': code,
                'title': basic_data[code],
                'description': basic_data[code],
                'version': 'ISIC4',
                'level': 'N/A',
                'parent': None
            }
        return None
    
    def _fallback_validate(self, code: str) -> bool:
        """Fallback validation when pyisic is not available"""
        basic_data = self._get_fallback_data()
        return code in basic_data
    
    def _get_fallback_data(self) -> Dict[str, str]:
        """Get comprehensive fallback ISIC data"""
        return {
            '0111': 'Growing of cereals (except rice), leguminous crops and oil seeds',
            '0112': 'Growing of rice',
            '0113': 'Growing of vegetables and melons, roots and tubers',
            '0114': 'Growing of sugar cane',
            '0115': 'Growing of tobacco',
            '0116': 'Growing of fibre crops',
            '0121': 'Growing of grapes',
            '0122': 'Growing of tropical and subtropical fruits',
            '0123': 'Growing of citrus fruits',
            '0124': 'Growing of pome fruits and stone fruits',
            '0125': 'Growing of other tree and bush fruits and nuts',
            '0126': 'Growing of oleaginous fruits',
            '0127': 'Growing of beverage crops',
            '0128': 'Growing of spices, aromatic, drug and pharmaceutical crops',
            '0129': 'Growing of other perennial crops',
            '0130': 'Plant propagation',
            '0141': 'Raising of cattle and buffaloes',
            '0142': 'Raising of horses and other equines',
            '0143': 'Raising of camels and camelids',
            '0144': 'Raising of sheep and goats',
            '0145': 'Raising of swine/pigs',
            '0146': 'Raising of poultry',
            '0149': 'Raising of other animals',
            '0150': 'Mixed farming',
            '0161': 'Support activities for crop production',
            '0162': 'Support activities for animal production',
            '0163': 'Post-harvest crop activities',
            '0164': 'Seed processing for propagation',
            '0170': 'Hunting, trapping and related service activities',
            '0210': 'Silviculture and other forestry activities',
            '0220': 'Logging',
            '0230': 'Gathering of non-wood forest products',
            '0240': 'Support services to forestry',
            '0311': 'Marine fishing',
            '0312': 'Freshwater fishing',
            '0321': 'Marine aquaculture',
            '0322': 'Freshwater aquaculture',
            '0510': 'Mining of hard coal',
            '0520': 'Mining of lignite',
            '0610': 'Extraction of crude petroleum',
            '0620': 'Extraction of natural gas',
            '0710': 'Mining of iron ores',
            '0721': 'Mining of uranium and thorium ores',
            '0729': 'Mining of other non-ferrous metal ores',
            '0810': 'Quarrying of stone, sand and clay',
            '0891': 'Mining of chemical and fertilizer minerals',
            '0892': 'Extraction of peat',
            '0893': 'Extraction of salt',
            '0899': 'Other mining and quarrying n.e.c.',
            '0910': 'Support activities for petroleum and natural gas extraction',
            '0990': 'Support activities for other mining and quarrying',
            '1010': 'Processing and preserving of meat',
            '1020': 'Processing and preserving of fish, crustaceans and molluscs',
            '1030': 'Processing and preserving of fruit and vegetables',
            '1040': 'Manufacture of vegetable and animal oils and fats',
            '1050': 'Manufacture of dairy products',
            '1061': 'Manufacture of grain mill products',
            '1062': 'Manufacture of starches and starch products',
            '1071': 'Manufacture of bakery products',
            '1072': 'Manufacture of sugar',
            '1073': 'Manufacture of cocoa, chocolate and sugar confectionery',
            '1074': 'Manufacture of macaroni, noodles, couscous and similar farinaceous products',
            '1075': 'Manufacture of prepared meals and dishes',
            '1079': 'Manufacture of other food products n.e.c.',
            '1080': 'Manufacture of prepared animal feeds',
            '1101': 'Distilling, rectifying and blending of spirits',
            '1102': 'Manufacture of wines',
            '1103': 'Manufacture of malt liquors and malt',
            '1104': 'Manufacture of soft drinks; production of mineral waters and other bottled waters',
            '1200': 'Manufacture of tobacco products',
            '1311': 'Preparation and spinning of textile fibres',
            '1312': 'Weaving of textiles',
            '1313': 'Finishing of textiles',
            '1391': 'Manufacture of knitted and crocheted fabrics',
            '1392': 'Manufacture of made-up textile articles, except apparel',
            '1393': 'Manufacture of carpets and rugs',
            '1394': 'Manufacture of cordage, rope, twine and netting',
            '1399': 'Manufacture of other textiles n.e.c.',
            '1410': 'Manufacture of wearing apparel, except fur apparel',
            '1420': 'Manufacture of articles of fur',
            '1430': 'Manufacture of knitted and crocheted apparel',
            '1511': 'Tanning and dressing of leather; dressing and dyeing of fur',
            '1512': 'Manufacture of luggage, handbags and the like, saddlery and harness',
            '1520': 'Manufacture of footwear',
            '1610': 'Sawmilling and planing of wood',
            '1621': 'Manufacture of veneer sheets and wood-based panels',
            '1622': 'Manufacture of builders carpentry and joinery',
            '1623': 'Manufacture of wooden containers',
            '1629': 'Manufacture of other products of wood; manufacture of articles of cork, straw and plaiting materials',
            '1701': 'Manufacture of pulp, paper and paperboard',
            '1702': 'Manufacture of corrugated paper and paperboard and of containers of paper and paperboard',
            '1709': 'Manufacture of other articles of paper and paperboard',
            '1811': 'Printing',
            '1812': 'Service activities related to printing',
            '1820': 'Reproduction of recorded media',
            '1910': 'Manufacture of coke oven products',
            '1920': 'Manufacture of refined petroleum products',
            '2011': 'Manufacture of basic chemicals',
            '2012': 'Manufacture of fertilizers and nitrogen compounds',
            '2013': 'Manufacture of plastics and synthetic rubber in primary forms',
            '2021': 'Manufacture of pesticides and other agrochemical products',
            '2022': 'Manufacture of paints, varnishes and similar coatings, printing ink and mastics',
            '2023': 'Manufacture of soap and detergents, cleaning and polishing preparations, perfumes and toilet preparations',
            '2029': 'Manufacture of other chemical products n.e.c.',
            '2030': 'Manufacture of man-made fibres',
            '2100': 'Manufacture of pharmaceuticals, medicinal chemical and botanical products',
            '2211': 'Manufacture of rubber tyres and tubes; retreading and rebuilding of rubber tyres',
            '2219': 'Manufacture of other rubber products',
            '2220': 'Manufacture of plastics products',
            '2310': 'Manufacture of glass and glass products',
            '2391': 'Manufacture of refractory products',
            '2392': 'Manufacture of clay building materials',
            '2393': 'Manufacture of other porcelain and ceramic products',
            '2394': 'Manufacture of cement, lime and plaster',
            '2395': 'Manufacture of articles of concrete, cement and plaster',
            '2396': 'Cutting, shaping and finishing of stone',
            '2399': 'Manufacture of other non-metallic mineral products n.e.c.',
            '2410': 'Manufacture of basic iron and steel',
            '2420': 'Manufacture of basic precious and other non-ferrous metals',
            '2431': 'Casting of iron and steel',
            '2432': 'Casting of non-ferrous metals',
            '2511': 'Manufacture of structural metal products',
            '2512': 'Manufacture of tanks, reservoirs and containers of metal',
            '2513': 'Manufacture of steam generators, except central heating hot water boilers',
            '2520': 'Manufacture of weapons and ammunition',
            '2591': 'Forging, pressing, stamping and roll-forming of metal; powder metallurgy',
            '2592': 'Treatment and coating of metals; machining',
            '2593': 'Manufacture of cutlery, hand tools and general hardware',
            '2599': 'Manufacture of other fabricated metal products n.e.c.',
            '2610': 'Manufacture of electronic components and boards',
            '2620': 'Manufacture of computers and peripheral equipment',
            '2630': 'Manufacture of communication equipment',
            '2640': 'Manufacture of consumer electronics',
            '2651': 'Manufacture of measuring, testing, navigating and control equipment',
            '2652': 'Manufacture of watches and clocks',
            '2660': 'Manufacture of irradiation, electromedical and electrotherapeutic equipment',
            '2670': 'Manufacture of optical instruments and photographic equipment',
            '2680': 'Manufacture of magnetic and optical media',
            '2710': 'Manufacture of electric motors, generators, transformers and electricity distribution and control apparatus',
            '2720': 'Manufacture of batteries and accumulators',
            '2731': 'Manufacture of fibre optic cables',
            '2732': 'Manufacture of other electronic and electric wires and cables',
            '2733': 'Manufacture of wiring devices',
            '2740': 'Manufacture of electric lighting equipment',
            '2750': 'Manufacture of domestic appliances',
            '2790': 'Manufacture of other electrical equipment',
            '2811': 'Manufacture of engines and turbines, except aircraft, vehicle and cycle engines',
            '2812': 'Manufacture of fluid power equipment',
            '2813': 'Manufacture of other pumps, compressors, taps and valves',
            '2814': 'Manufacture of bearings, gears, gearing and driving elements',
            '2815': 'Manufacture of ovens, furnaces and furnace burners',
            '2816': 'Manufacture of lifting and handling equipment',
            '2817': 'Manufacture of office machinery and equipment (except computers and peripheral equipment)',
            '2818': 'Manufacture of power-driven hand tools',
            '2819': 'Manufacture of other general-purpose machinery',
            '2821': 'Manufacture of agricultural and forestry machinery',
            '2822': 'Manufacture of metal-forming machinery and machine tools',
            '2823': 'Manufacture of machinery for metallurgy',
            '2824': 'Manufacture of machinery for mining, quarrying and construction',
            '2825': 'Manufacture of machinery for food, beverage and tobacco processing',
            '2826': 'Manufacture of machinery for textile, apparel and leather production',
            '2829': 'Manufacture of other special-purpose machinery',
            '2910': 'Manufacture of motor vehicles',
            '2920': 'Manufacture of bodies (coachwork) for motor vehicles; manufacture of trailers and semi-trailers',
            '2930': 'Manufacture of parts and accessories for motor vehicles',
            '3011': 'Building of ships and floating structures',
            '3012': 'Building of pleasure and sporting boats',
            '3020': 'Manufacture of railway locomotives and rolling stock',
            '3030': 'Manufacture of air and spacecraft and related machinery',
            '3040': 'Manufacture of military fighting vehicles',
            '3091': 'Manufacture of motorcycles',
            '3092': 'Manufacture of bicycles and invalid carriages',
            '3099': 'Manufacture of other transport equipment n.e.c.',
            '3100': 'Manufacture of furniture',
            '3211': 'Manufacture of jewellery and related articles',
            '3212': 'Manufacture of imitation jewellery and related articles',
            '3220': 'Manufacture of musical instruments',
            '3230': 'Manufacture of sports goods',
            '3240': 'Manufacture of games and toys',
            '3250': 'Manufacture of medical and dental instruments and supplies',
            '3290': 'Other manufacturing n.e.c.',
            '3311': 'Repair of fabricated metal products',
            '3312': 'Repair of machinery',
            '3313': 'Repair of electronic and optical equipment',
            '3314': 'Repair of electrical equipment',
            '3315': 'Repair of transport equipment, except motor vehicles',
            '3319': 'Repair of other equipment',
            '3320': 'Installation of industrial machinery and equipment',
            '3510': 'Electric power generation, transmission and distribution',
            '3520': 'Manufacture of gas; distribution of gaseous fuels through mains',
            '3530': 'Steam and air conditioning supply',
            '3600': 'Water collection, treatment and supply',
            '3700': 'Sewerage',
            '3811': 'Collection of non-hazardous waste',
            '3812': 'Collection of hazardous waste',
            '3821': 'Treatment and disposal of non-hazardous waste',
            '3822': 'Treatment and disposal of hazardous waste',
            '3830': 'Materials recovery',
            '3900': 'Remediation activities and other waste management services',
            '4110': 'Development of building projects',
            '4120': 'Construction of buildings',
            '4210': 'Construction of roads and railways',
            '4220': 'Construction of utility projects',
            '4290': 'Construction of other civil engineering projects',
            '4311': 'Demolition',
            '4312': 'Site preparation',
            '4321': 'Electrical installation',
            '4322': 'Plumbing, heat and air-conditioning installation',
            '4329': 'Other construction installation',
            '4330': 'Building completion and finishing',
            '4390': 'Other specialized construction activities',
            '4510': 'Sale of motor vehicles',
            '4520': 'Maintenance and repair of motor vehicles',
            '4530': 'Sale of motor vehicle parts and accessories',
            '4540': 'Sale, maintenance and repair of motorcycles and related parts and accessories',
            '4610': 'Wholesale on a fee or contract basis',
            '4620': 'Wholesale of agricultural raw materials and live animals',
            '4630': 'Wholesale of food, beverages and tobacco',
            '4641': 'Wholesale of textiles, clothing and footwear',
            '4649': 'Wholesale of other household goods',
            '4651': 'Wholesale of computers, computer peripheral equipment and software',
            '4652': 'Wholesale of electronic and telecommunications equipment and parts',
            '4653': 'Wholesale of agricultural machinery, equipment and supplies',
            '4659': 'Wholesale of other machinery and equipment',
            '4661': 'Wholesale of solid, liquid and gaseous fuels and related products',
            '4662': 'Wholesale of metals and metal ores',
            '4663': 'Wholesale of construction materials, hardware, plumbing and heating equipment and supplies',
            '4669': 'Wholesale of waste and scrap and other products n.e.c.',
            '4690': 'Non-specialized wholesale trade',
            '4711': 'Retail sale in non-specialized stores with food, beverages or tobacco predominating',
            '4719': 'Other retail sale in non-specialized stores',
            '4721': 'Retail sale of food in specialized stores',
            '4722': 'Retail sale of beverages in specialized stores',
            '4723': 'Retail sale of tobacco products in specialized stores',
            '4730': 'Retail sale of automotive fuel in specialized stores',
            '4741': 'Retail sale of computers, peripheral units, software and telecommunications equipment in specialized stores',
            '4742': 'Retail sale of audio and video equipment in specialized stores',
            '4751': 'Retail sale of textiles in specialized stores',
            '4752': 'Retail sale of hardware, paints and glass in specialized stores',
            '4753': 'Retail sale of carpets, rugs, wall and floor coverings in specialized stores',
            '4759': 'Retail sale of electrical household appliances, furniture, lighting equipment and other household articles in specialized stores',
            '4761': 'Retail sale of books, newspapers and stationary in specialized stores',
            '4762': 'Retail sale of music and video recordings in specialized stores',
            '4763': 'Retail sale of sporting equipment in specialized stores',
            '4764': 'Retail sale of games and toys in specialized stores',
            '4771': 'Retail sale of clothing, footwear and leather articles in specialized stores',
            '4772': 'Retail sale of pharmaceutical and medical goods, cosmetic and toilet articles in specialized stores',
            '4773': 'Other retail sale of new goods in specialized stores',
            '4774': 'Retail sale of second-hand goods',
            '4781': 'Retail sale via stalls and markets of food, beverages and tobacco products',
            '4782': 'Retail sale via stalls and markets of textiles, clothing and footwear',
            '4789': 'Retail sale via stalls and markets of other goods',
            '4791': 'Retail sale via mail order houses or via Internet',
            '4799': 'Other retail sale not in stores, stalls or markets',
            '4911': 'Passenger rail transport, interurban',
            '4912': 'Freight rail transport',
            '4921': 'Urban and suburban passenger land transport',
            '4922': 'Other passenger land transport',
            '4923': 'Freight transport by road',
            '4930': 'Transport via pipeline',
            '5011': 'Sea and coastal passenger water transport',
            '5012': 'Sea and coastal freight water transport',
            '5021': 'Inland passenger water transport',
            '5022': 'Inland freight water transport',
            '5110': 'Passenger air transport',
            '5120': 'Freight air transport',
            '5210': 'Warehousing and storage',
            '5221': 'Service activities incidental to land transportation',
            '5222': 'Service activities incidental to water transportation',
            '5223': 'Service activities incidental to air transportation',
            '5224': 'Cargo handling',
            '5229': 'Other transportation support activities',
            '5310': 'Postal activities',
            '5320': 'Courier activities',
            '5510': 'Short term accommodation activities',
            '5520': 'Camping grounds, recreational vehicle parks and trailer parks',
            '5590': 'Other accommodation',
            '5610': 'Restaurants and mobile food service activities',
            '5621': 'Event catering',
            '5629': 'Other food service activities',
            '5630': 'Beverage serving activities',
            '5811': 'Book publishing',
            '5812': 'Publishing of directories and mailing lists',
            '5813': 'Publishing of newspapers, journals and periodicals',
            '5819': 'Other publishing activities',
            '5820': 'Software publishing',
            '5911': 'Motion picture, video and television programme production activities',
            '5912': 'Motion picture, video and television programme post-production activities',
            '5913': 'Motion picture, video and television programme distribution activities',
            '5914': 'Motion picture projection activities',
            '5920': 'Sound recording and music publishing activities',
            '6010': 'Radio broadcasting',
            '6020': 'Television programming and broadcasting activities',
            '6110': 'Wired telecommunications activities',
            '6120': 'Wireless telecommunications activities',
            '6130': 'Satellite telecommunications activities',
            '6190': 'Other telecommunications activities',
            '6201': 'Computer programming activities',
            '6202': 'Computer consultancy activities',
            '6209': 'Other information technology and computer service activities',
            '6311': 'Data processing, hosting and related activities',
            '6312': 'Web portals',
            '6391': 'News agency activities',
            '6399': 'Other information service activities n.e.c.',
            '6411': 'Central banking',
            '6419': 'Other monetary intermediation',
            '6420': 'Activities of holding companies',
            '6430': 'Trusts, funds and similar financial entities',
            '6491': 'Financial leasing',
            '6492': 'Other credit granting',
            '6499': 'Other financial service activities, except insurance and pension funding n.e.c.',
            '6511': 'Life insurance',
            '6512': 'Non-life insurance',
            '6520': 'Reinsurance',
            '6530': 'Pension funding',
            '6611': 'Administration of financial markets',
            '6612': 'Security and commodity contracts brokerage',
            '6619': 'Other activities auxiliary to financial service activities',
            '6621': 'Risk and damage evaluation',
            '6622': 'Activities of insurance agents and brokers',
            '6629': 'Other activities auxiliary to insurance and pension funding',
            '6630': 'Fund management activities',
            '6810': 'Real estate activities with own or leased property',
            '6820': 'Real estate activities on a fee or contract basis',
            '6910': 'Legal activities',
            '6920': 'Accounting, bookkeeping and auditing activities; tax consultancy',
            '7010': 'Activities of head offices',
            '7020': 'Management consultancy activities',
            '7110': 'Architectural and engineering activities and related technical consultancy',
            '7120': 'Technical testing and analysis',
            '7210': 'Research and experimental development on natural sciences and engineering',
            '7220': 'Research and experimental development on social sciences and humanities',
            '7310': 'Advertising',
            '7320': 'Market research and public opinion polling',
            '7410': 'Specialized design activities',
            '7420': 'Photographic activities',
            '7490': 'Other professional, scientific and technical activities n.e.c.',
            '7500': 'Veterinary activities',
            '7710': 'Renting and leasing of motor vehicles',
            '7721': 'Renting and leasing of recreational and sports goods',
            '7722': 'Renting of video tapes and disks',
            '7729': 'Renting and leasing of other personal and household goods',
            '7730': 'Renting and leasing of other machinery, equipment and tangible goods',
            '7740': 'Leasing of intellectual property and similar products, except copyrighted works',
            '7810': 'Activities of employment placement agencies',
            '7820': 'Temporary employment agency activities',
            '7830': 'Other human resources provision',
            '7911': 'Travel agency activities',
            '7912': 'Tour operator activities',
            '7990': 'Other reservation service and related activities',
            '8010': 'Private security activities',
            '8020': 'Security systems service activities',
            '8030': 'Investigation activities',
            '8110': 'Combined facilities support activities',
            '8121': 'General cleaning of buildings',
            '8129': 'Other building and industrial cleaning activities',
            '8130': 'Landscape care and maintenance service activities',
            '8211': 'Combined office administrative service activities',
            '8219': 'Photocopying, document preparation and other specialized office support activities',
            '8220': 'Activities of call centres',
            '8230': 'Organization of conventions and trade shows',
            '8291': 'Activities of collection agencies and credit bureaus',
            '8292': 'Packaging activities',
            '8299': 'Other business support service activities n.e.c.',
            '8411': 'General public administration activities',
            '8412': 'Regulation of the activities of providing health care, education, cultural services and other social services, excluding social security',
            '8413': 'Regulation of and contribution to more efficient operation of businesses',
            '8421': 'Foreign affairs',
            '8422': 'Defence activities',
            '8423': 'Public order and safety activities',
            '8430': 'Compulsory social security activities',
            '8510': 'Pre-primary education',
            '8520': 'Primary education',
            '8530': 'Secondary education',
            '8541': 'Post-secondary non-tertiary education',
            '8542': 'Tertiary education',
            '8550': 'Other education',
            '8560': 'Educational support activities',
            '8610': 'Hospital activities',
            '8620': 'Medical and dental practice activities',
            '8690': 'Other human health activities',
            '8710': 'Residential nursing care facilities',
            '8720': 'Residential care activities for mental retardation, mental health and substance abuse',
            '8730': 'Residential care activities for the elderly and disabled',
            '8790': 'Other residential care activities',
            '8810': 'Social work activities without accommodation for the elderly and disabled',
            '8890': 'Other social work activities without accommodation',
            '9000': 'Creative, arts and entertainment activities',
            '9101': 'Library and archives activities',
            '9102': 'Museums activities and operation of historical sites and buildings',
            '9103': 'Botanical and zoological gardens and nature reserves activities',
            '9200': 'Gambling and betting activities',
            '9311': 'Operation of sports facilities',
            '9312': 'Activities of sports clubs',
            '9319': 'Other sports activities',
            '9321': 'Activities of amusement parks and theme parks',
            '9329': 'Other amusement and recreation activities',
            '9411': 'Activities of business and employers membership organizations',
            '9412': 'Activities of professional membership organizations',
            '9420': 'Activities of trade unions',
            '9491': 'Activities of religious organizations',
            '9492': 'Activities of political organizations',
            '9499': 'Activities of other membership organizations n.e.c.',
            '9511': 'Repair of computers and peripheral equipment',
            '9512': 'Repair of communication equipment',
            '9521': 'Repair of consumer electronics',
            '9522': 'Repair of household appliances and home and garden equipment',
            '9523': 'Repair of footwear and leather goods',
            '9524': 'Repair of furniture and home furnishings',
            '9529': 'Repair of other personal and household goods',
            '9601': 'Washing and (dry-) cleaning of textile and fur products',
            '9602': 'Hairdressing and other beauty treatment',
            '9603': 'Funeral and related activities',
            '9609': 'Other personal service activities n.e.c.',
            '9700': 'Activities of households as employers of domestic personnel',
            '9810': 'Undifferentiated goods-producing activities of private households for own use',
            '9820': 'Undifferentiated service-producing activities of private households for own use',
            '9900': 'Activities of extraterritorial organizations and bodies'
        }

# Enhanced ISIC data management for SQLite Cloud
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
                
                # Cache the data in SQLite Cloud
                cache_isic_data(df)
                return df
            except Exception as e:
                st.warning(f"Could not load {filename}: {str(e)}")
                continue
    
    # If no file found, try to load from cache
    return load_isic_from_cache()

def cache_isic_data(df):
    """Cache ISIC data in SQLite Cloud for faster access"""
    try:
        # Clear existing cache
        execute_query("DELETE FROM isic_cache")
        
        # Insert new data
        insert_data = []
        for _, row in df.iterrows():
            code_col = next((col for col in ['Code', 'CODE', 'code'] if col in df.columns), None)
            title_col = next((col for col in ['Title', 'TITLE', 'title'] if col in df.columns), None)
            
            if code_col and title_col:
                code = str(row[code_col]) if pd.notna(row[code_col]) else None
                title = str(row[title_col]) if pd.notna(row[title_col]) else ""
                description = title
                
                if code:
                    current_time = datetime.now().isoformat()
                    insert_data.append((code, title, description, 'General', current_time))
        
        # Batch insert
        if insert_data:
            execute_many(
                "INSERT INTO isic_cache (code, title, description, category, last_updated) VALUES (?, ?, ?, ?, ?)",
                insert_data
            )
            st.success(f"✅ Cached {len(insert_data)} ISIC codes in database")
        
    except Exception as e:
        st.warning(f"Could not cache ISIC data: {str(e)}")

def load_isic_from_cache():
    """Load ISIC data from SQLite Cloud cache"""
    try:
        result = execute_query("SELECT code, title, description FROM isic_cache", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            result_data, columns = result
            df = pd.DataFrame(result_data, columns=columns)
            
            if not df.empty:
                st.info("📁 Loaded ISIC data from cache")
                return df
            else:
                st.warning("No ISIC data available in cache. Using basic codes.")
                return create_basic_isic_data()
        else:
            st.warning("No ISIC data available. Using basic codes.")
            return create_basic_isic_data()
    except Exception as e:
        st.warning(f"Error loading ISIC cache: {str(e)}")
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

# Enhanced Business Activities with ISIC Integration for SQLite Cloud
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

# Initialize database - FIXED VERSION
def init_db():
    """Initialize database tables in SQLite Cloud"""
    conn = get_connection()
    if conn is None:
        st.error("❌ Cannot connect to database")
        return False
    
    try:
        c = conn.cursor()
        
        # Check if tables exist first
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='responses'")
        table_exists = c.fetchone()
        
        if not table_exists:
            st.info("🔄 Creating database tables...")
            
            # Main responses table
            c.execute('''
                CREATE TABLE responses (
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
                    draft_progress REAL DEFAULT 0
                )
            ''')
            
            # Additional tables
            c.execute('''
                CREATE TABLE IF NOT EXISTS isic_cache (
                    code TEXT PRIMARY KEY,
                    title TEXT,
                    description TEXT,
                    category TEXT,
                    last_updated TIMESTAMP
                )
            ''')
            
            c.execute('''
                CREATE TABLE IF NOT EXISTS admin_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT,
                    action TEXT,
                    timestamp TIMESTAMP,
                    details TEXT
                )
            ''')
            
            c.execute('''
                CREATE TABLE IF NOT EXISTS user_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT,
                    login_time TIMESTAMP,
                    logout_time TIMESTAMP,
                    session_duration INTEGER
                )
            ''')
            
            c.execute('''
                CREATE TABLE IF NOT EXISTS edit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT,
                    interview_id TEXT,
                    action TEXT,
                    changes TEXT,
                    timestamp TIMESTAMP
                )
            ''')
            
            conn.commit()
            st.success("✅ Database tables created successfully!")
        else:
            st.success("✅ Database tables already exist!")
        
        return True
        
    except Exception as e:
        st.error(f"❌ Database initialization error: {str(e)}")
        return False
    finally:
        conn.close()

def check_and_fix_database():
    """Check database schema and fix if needed"""
    try:
        # First try to initialize the database
        if not init_db():
            return False
        
        # Check if responses table exists
        result = execute_query("SELECT name FROM sqlite_master WHERE type='table' AND name='responses'", return_result=True)
        
        if not result or not result[0] or len(result[0]) == 0:
            st.warning("📊 Database tables not found. Creating...")
            return init_db()
        
        return True
    except Exception as e:
        st.error(f"Error checking database: {str(e)}")
        return init_db()

def add_missing_columns():
    """Add missing columns to existing database tables"""
    try:
        # First ensure database is properly initialized
        if not check_and_fix_database():
            return False
            
        # Get current columns
        result = execute_query("PRAGMA table_info(responses)", return_result=True)
        if result and isinstance(result, tuple) and result[0]:
            columns = [column[1] for column in result[0]]
            
            missing_columns = []
            
            # Check for all required columns
            required_columns = [
                'created_by', 'current_section', 'draft_progress'
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
                
                st.success("✅ Database schema updated successfully!")
                
        return True
    except Exception as e:
        st.warning(f"Database schema update: {str(e)}")
        return False

# Database functions
def save_draft(data, interview_id=None):
    """Save form data as draft"""
    try:
        if not interview_id:
            interview_id = generate_interview_id()
        
        # Calculate total compliance metrics
        procedure_data = data.get('procedure_data', [])
        total_cost = sum(proc.get('official_fees', 0) + proc.get('unofficial_payments', 0) for proc in procedure_data)
        total_time = sum(proc.get('total_days', 0) for proc in procedure_data)
        
        # Calculate risk score
        risk_score = min((total_cost / 100000 + total_time / 365) * 10, 10)
        
        # Calculate draft progress
        draft_manager = DraftManager()
        progress = draft_manager.calculate_progress(data, st.session_state.current_section)
        
        # Prepare data
        isic_codes = data.get('isic_codes', [])
        reform_priorities = data.get('reform_priorities', [])
        procedure_data_json = json.dumps(procedure_data)
        
        current_time = datetime.now().isoformat()
        
        # Check if record exists
        existing_result = execute_query("SELECT id FROM responses WHERE interview_id = ?", (interview_id,), return_result=True)
        
        if existing_result and isinstance(existing_result, tuple) and existing_result[0]:
            # Update existing draft
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
                    current_section=?, draft_progress=?
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
                interview_id
            )
            result = execute_query(update_query, params)
        else:
            # Insert new draft
            insert_query = '''
                INSERT INTO responses (
                    interview_id, interviewer_name, interview_date, start_time, end_time,
                    business_name, district, physical_address, contact_person, email,
                    phone, primary_sector, legal_status, business_size, ownership_structure,
                    gender_owner, business_activities, isic_codes, year_established,
                    turnover_range, employees_fulltime, employees_parttime, procedure_data,
                    completion_time_local, completion_time_national, completion_time_dk,
                    compliance_cost_percentage, permit_comparison_national,
                    permit_comparison_local, cost_comparison_national, cost_comparison_local,
                    business_climate_rating, reform_priorities, status, submission_date,
                    last_modified, total_compliance_cost, total_compliance_time, risk_score,
                    created_by, current_section, draft_progress
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                progress
            )
            result = execute_query(insert_query, insert_data)
        
        if result:
            return interview_id
        return None
        
    except Exception as e:
        st.error(f"Error saving draft: {str(e)}")
        return None

def check_duplicate_business_name(business_name, current_interview_id=None):
    """Check if business name already exists"""
    try:
        if current_interview_id:
            result = execute_query("SELECT COUNT(*) FROM responses WHERE business_name = ? AND interview_id != ?", 
                                 (business_name, current_interview_id), return_result=True)
        else:
            result = execute_query("SELECT COUNT(*) FROM responses WHERE business_name = ?", (business_name,), return_result=True)
        
        if result and isinstance(result, tuple) and result[0]:
            count = result[0][0][0] if result[0] else 0
            return count > 0
        return False
    except Exception as e:
        st.error(f"Error checking duplicate business name: {str(e)}")
        return False

def submit_final(interview_id):
    """Mark draft as final submission"""
    try:
        business_name = st.session_state.form_data.get('business_name', '')
        if check_duplicate_business_name(business_name, interview_id):
            st.error(f"❌ Business name '{business_name}' already exists. Please use a unique name.")
            return False
        
        current_time = datetime.now().isoformat()
        result = execute_query("UPDATE responses SET status = 'submitted', submission_date = ? WHERE interview_id = ?", 
                              (current_time, interview_id))
        
        if result:
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
    """Get all interviews from database"""
    try:
        # First ensure database is properly initialized
        if not check_and_fix_database():
            return pd.DataFrame()
            
        query = """
        SELECT 
            interview_id, business_name, district, primary_sector, 
            business_size, status, submission_date, last_modified,
            total_compliance_cost, total_compliance_time, risk_score, created_by
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
    """Get interviews created by specific user"""
    try:
        # First ensure database is properly initialized
        if not check_and_fix_database():
            return pd.DataFrame()
            
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

# Authentication System
def login_system():
    """Enhanced login system"""
    st.title("🔐 Zambia Regulatory Compliance Survey")
    st.subheader("Login to Access the System")
    
    # Initialize database on first load
    if not st.session_state.get('database_initialized', False):
        with st.spinner("🔄 Initializing database..."):
            if check_and_fix_database():
                st.session_state.database_initialized = True
                st.success("✅ Database initialized successfully!")
            else:
                st.error("❌ Failed to initialize database")
    
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
                
                log_user_session(username, datetime.now().isoformat())
                log_admin_action(username, "login")
                
                st.success(f"Welcome {username}! Login successful!")
                st.rerun()
            else:
                st.error("Invalid credentials. Please try again.")

def logout():
    """Logout function"""
    if st.session_state.current_user:
        log_user_session(
            st.session_state.current_user, 
            datetime.now().isoformat(), 
            datetime.now().isoformat(),
            0
        )
        log_admin_action(st.session_state.current_user, "logout")
    
    # Reset session states
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    
    initialize_session_state()
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

# Section A - Business Profile
def display_section_a():
    """Section A: Interview & Business Profile"""
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
            if business_name and st.session_state.current_interview_id:
                if check_duplicate_business_name(business_name, st.session_state.current_interview_id):
                    st.error(f"⚠️ Business name '{business_name}' already exists. Please use a unique name.")
            
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
        
        st.subheader("A4. Business Background")
        
        business_activities = st.text_area(
            "Business Activities Description *",
            value=st.session_state.business_activities_text,
            placeholder="Describe your main business activities, products, and services in detail...",
            height=120,
            key="business_activities_form"
        )
        st.session_state.business_activities_text = business_activities
        
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
            if not business_name:
                st.error("❌ Business Name is required!")
                return
                
            if check_duplicate_business_name(business_name, st.session_state.current_interview_id):
                st.error(f"❌ Business name '{business_name}' already exists. Please use a unique name.")
                return
            
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
    
    # ISIC Code section
    st.markdown("---")
    business_activities_section()

# Section B - Registration & Licensing
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

def quick_manual_procedure():
    """Quick manual procedure entry"""
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

def single_procedure_capture():
    """Single procedure detailed capture"""
    st.subheader("🔧 Detailed Procedure Analysis")
    
    with st.form("single_procedure_form"):
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
        
        st.write("⏱️ Time Analysis")
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
        
        st.write("💰 Cost Analysis")
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
        
        st.write("🛠️ External Support")
        support_col1, support_col2, support_col3 = st.columns(3)
        
        with support_col1:
            external_support = st.radio("Hired External Support?", ["No", "Yes"], 
                                      horizontal=True, key="external_support_single")
        
        if 'external_support_active' not in st.session_state:
            st.session_state.external_support_active = False
        
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
                st.number_input("Support Cost (ZMW)", min_value=0.0, value=0.0, 
                              disabled=True, key="external_cost_disabled")
        
        with support_col3:
            if st.session_state.external_support_active:
                external_reason = st.selectbox("Primary Reason", 
                                             ["Saves Time", "Expertise Required", "Connections/Relationships", "Complexity"],
                                             key="external_reason_single")
            else:
                external_reason = ""
                st.selectbox("Primary Reason", 
                           ["Saves Time", "Expertise Required", "Connections/Relationships", "Complexity"],
                           disabled=True, key="external_reason_disabled")
        
        st.write("📊 Assessment")
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
        
        st.write("📄 Requirements & Challenges")
        
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

def enhanced_bulk_procedures_capture():
    """Enhanced bulk capture with more options"""
    st.subheader("📊 Enhanced Bulk Procedure Capture")
    
    sector = st.session_state.form_data.get('primary_sector', 'Agribusiness')
    
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
            }
        }
    }
    
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
    
    with st.form("enhanced_bulk_form"):
        st.write("**📋 Bulk License Selection**")
        
        sector_licenses = expanded_licenses.get(sector, {})
        selected_licenses = []
        
        for license_name, license_data in sector_licenses.items():
            if st.checkbox(f"{license_name} ({license_data['authority']})", key=f"bulk_{license_name}"):
                selected_licenses.append((license_name, license_data))
        
        if selected_licenses:
            st.write("**⚙️ Bulk Configuration**")
            config_col1, config_col2 = st.columns(2)
            
            with config_col1:
                bulk_status = st.selectbox("Status for all", ["Completed", "In Progress", "Not Started"], key="bulk_status")
                bulk_mode = st.selectbox("Application Mode for all", APPLICATION_MODES, key="bulk_mode")
            
            with config_col2:
                cost_adjust = st.number_input("Cost Adjustment (%)", min_value=-100, max_value=100, value=0, key="cost_adj")
                time_adjust = st.number_input("Time Adjustment (%)", min_value=-50, max_value=200, value=0, key="time_adj")
        
        if st.form_submit_button("📥 Add Selected Licenses", use_container_width=True):
            added_count = 0
            for license_name, license_data in selected_licenses:
                template_data = expanded_licenses[sector][license_name]
                
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
                    'documents': template_data['common_documents'],
                    'challenges': '',
                    'follow_ups': 2
                }
                
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

def interactive_procedures_manager():
    """Manage procedures with enhanced editing"""
    if not st.session_state.procedures_list:
        st.info("📝 No procedures added yet. Use the forms above to add procedures.")
        return
    
    st.subheader("📋 Procedures Management")
    
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
    
    for i, procedure in enumerate(st.session_state.procedures_list):
        with st.container():
            st.markdown(f"**{i+1}. {procedure['procedure']}** - {procedure['authority']} ({procedure['status']})")
            
            col1, col2 = st.columns([3, 1])
            
            with col1:
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
            
            with col2:
                if st.button("✏️ Edit", key=f"edit_proc_{i}"):
                    st.session_state.active_procedure_index = i
                
                if st.button("🗑️ Delete", key=f"delete_proc_{i}"):
                    st.session_state.procedures_list.pop(i)
                    st.rerun()
            
            if st.session_state.get('active_procedure_index') == i:
                with st.form(f"edit_procedure_{i}"):
                    st.write("**Edit Procedure**")
                    
                    edit_col1, edit_col2 = st.columns(2)
                    with edit_col1:
                        new_status = st.selectbox("Status", ["Not Started", "In Progress", "Completed", "Delayed", "Rejected"], 
                                                index=["Not Started", "In Progress", "Completed", "Delayed", "Rejected"].index(procedure['status']),
                                                key=f"edit_status_{i}")
                        new_complexity = st.slider("Complexity", 1, 5, procedure['complexity'],
                                                 key=f"edit_complexity_{i}")
                    with edit_col2:
                        new_official_fees = st.number_input("Official Fees", min_value=0.0, value=procedure['official_fees'],
                                                          key=f"edit_fees_{i}")
                        new_unofficial = st.number_input("Unofficial Payments", min_value=0.0, value=procedure.get('unofficial_payments', 0.0),
                                                       key=f"edit_unofficial_{i}")
                    
                    new_application_mode = st.selectbox("Application Mode", APPLICATION_MODES, 
                                                      index=APPLICATION_MODES.index(procedure['application_mode']) if procedure['application_mode'] in APPLICATION_MODES else 0,
                                                      key=f"edit_app_mode_{i}")
                    
                    col1, col2 = st.columns(2)
                    with col1:
                        if st.form_submit_button("💾 Save Changes", use_container_width=True):
                            st.session_state.procedures_list[i].update({
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
            
            st.markdown("---")

def generate_procedures_report():
    """Generate a quick procedures report"""
    if not st.session_state.procedures_list:
        st.warning("No procedures to report")
        return
    
    total_cost = sum(p['official_fees'] + p.get('unofficial_payments', 0) for p in st.session_state.procedures_list)
    total_time = sum(p['total_days'] for p in st.session_state.procedures_list)
    avg_complexity = sum(p['complexity'] for p in st.session_state.procedures_list) / len(st.session_state.procedures_list)
    
    st.subheader("📈 Quick Procedures Report")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Procedures", len(st.session_state.procedures_list))
    with col2:
        st.metric("Total Cost", f"ZMW {total_cost:,.0f}")
    with col3:
        st.metric("Total Time", f"{total_time} days")
    with col4:
        st.metric("Avg Complexity", f"{avg_complexity:.1f}/5")

# Section C - Ongoing Compliance
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

# Section D - Reform Priorities
def display_section_d():
    """Section D - Reform Priorities"""
    st.header("💡 SECTION D: Reform Priorities & Recommendations")
    
    with st.form("section_d_form"):
        st.subheader("Reform Recommendations")
        
        st.write("""
        *If you could advise the government on specific, actionable reforms 
        to reduce the compliance burden, what would they be?*
        """)
        
        st.write("**🎯 Top Reform Priorities**")
        
        reform_options = [
            "Simplify application procedures",
            "Reduce number of required documents",
            "Standardize forms across agencies",
            "Create single-window clearance system",
            "Set maximum processing time limits",
            "Implement online tracking systems",
            "Lower official fees for small businesses",
            "Provide fee waivers for startups",
            "Full online application system",
            "Digital document submission",
            "Publish clear requirements online",
            "Provide status updates automatically",
            "Better inter-agency coordination",
            "Training for regulatory staff"
        ]
        
        selected_reforms = []
        for reform in reform_options:
            if st.checkbox(reform, key=f"reform_{reform}"):
                selected_reforms.append(reform)
        
        st.subheader("💡 Additional Custom Recommendations")
        custom_reforms = st.text_area(
            "Enter additional reform priorities:",
            placeholder="Add your own specific recommendations...",
            height=100,
            key="custom_reforms"
        )
        
        if custom_reforms:
            custom_list = [r.strip() for r in custom_reforms.split('\n') if r.strip()]
            selected_reforms.extend(custom_list)
        
        col1, col2 = st.columns(2)
        with col1:
            save_btn = st.form_submit_button("💾 Save Section D", use_container_width=True)
        with col2:
            submit_btn = st.form_submit_button("🚀 Submit Complete Interview", use_container_width=True)
        
        if save_btn:
            st.session_state.form_data['reform_priorities'] = selected_reforms
            interview_id = save_draft(st.session_state.form_data, st.session_state.current_interview_id)
            if interview_id:
                st.session_state.current_interview_id = interview_id
                st.success("✅ Section D saved successfully!")
        
        if submit_btn:
            st.session_state.form_data['reform_priorities'] = selected_reforms
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
        if st.button("📊 View Analysis", use_container_width=True, key="view_analysis_btn"):
            st.session_state.current_section = 'Dashboard'
            st.rerun()
    
    with col2:
        if st.button("📋 Start New Interview", use_container_width=True, key="new_interview_complete_btn"):
            reset_interview()
    
    with col3:
        if st.button("🏠 Return to Dashboard", use_container_width=True, key="return_dashboard_btn"):
            st.session_state.current_section = 'Dashboard'
            st.rerun()

# Main application function
def main():
    """Main application function"""
    
    # Display login or main app
    if not st.session_state.logged_in:
        display_login()
    else:
        display_main_app()

def display_login():
    """Display login interface"""
    st.title("🇿🇲 Zambia Regulatory Compliance Survey")
    st.markdown("---")
    
    tab1, tab2 = st.tabs(["📝 Interviewer Login", "👨‍💼 Admin/Researcher Login"])
    
    with tab1:
        st.subheader("Interviewer Access")
        interviewer = st.selectbox("Select Interviewer", INTERVIEWERS)
        password = st.text_input("Password", type="password", key="interviewer_pwd")
        
        if st.button("Login as Interviewer", type="primary", use_container_width=True):
            if (interviewer in INTERVIEWER_CREDENTIALS and 
                password == INTERVIEWER_CREDENTIALS[interviewer]["password"]):
                st.session_state.logged_in = True
                st.session_state.current_user = interviewer
                st.session_state.user_role = "interviewer"
                st.session_state.interviewer_logged_in = True
                st.success(f"✅ Welcome {interviewer}!")
                st.rerun()
            else:
                st.error("❌ Invalid credentials")
    
    with tab2:
        st.subheader("Admin/Researcher Access")
        admin_user = st.text_input("Username", key="admin_user")
        admin_password = st.text_input("Password", type="password", key="admin_pwd")
        
        if st.button("Login as Admin/Researcher", type="primary", use_container_width=True):
            if (admin_user in ADMIN_CREDENTIALS and 
                admin_password == ADMIN_CREDENTIALS[admin_user]["password"]):
                st.session_state.logged_in = True
                st.session_state.current_user = admin_user
                st.session_state.user_role = ADMIN_CREDENTIALS[admin_user]["role"]
                st.session_state.admin_logged_in = True
                st.success(f"✅ Welcome {admin_user}!")
                st.rerun()
            else:
                st.error("❌ Invalid credentials")

def display_main_app():
    """Display main application after login"""
    
    # Sidebar navigation
    with st.sidebar:
        st.title(f"Welcome, {st.session_state.current_user}!")
        st.markdown(f"*Role: {st.session_state.user_role}*")
        st.markdown("---")
        
        # Navigation based on user role
        if st.session_state.interviewer_logged_in:
            st.subheader("📋 Survey Navigation")
            
            nav_options = [
                "🏠 Dashboard",
                "📝 Section A: Business Information",
                "📊 Section B: Regulatory Procedures", 
                "⏱️ Section C: Compliance Time & Cost",
                "💡 Section D: Reform Priorities",
                "📋 Draft Management"
            ]
            
            selected_nav = st.radio("Go to:", nav_options)
            
            # Map navigation to sections
            if selected_nav == "🏠 Dashboard":
                st.session_state.current_section = 'Dashboard'
            elif selected_nav == "📝 Section A: Business Information":
                st.session_state.current_section = 'A'
            elif selected_nav == "📊 Section B: Regulatory Procedures":
                st.session_state.current_section = 'B'
            elif selected_nav == "⏱️ Section C: Compliance Time & Cost":
                st.session_state.current_section = 'C'
            elif selected_nav == "💡 Section D: Reform Priorities":
                st.session_state.current_section = 'D'
            elif selected_nav == "📋 Draft Management":
                st.session_state.current_section = 'Draft_Dashboard'
        
        elif st.session_state.admin_logged_in:
            st.subheader("👨‍💼 Admin Navigation")
            
            admin_nav_options = [
                "📊 Data Overview",
                "👥 Interviewer Management", 
                "📋 Draft Management",
                "📈 Analytics & Reports",
                "⚙️ System Settings"
            ]
            
            selected_admin_nav = st.radio("Go to:", admin_nav_options)
            
            # Map admin navigation
            if selected_admin_nav == "📊 Data Overview":
                st.session_state.current_section = 'Admin_Dashboard'
            elif selected_admin_nav == "👥 Interviewer Management":
                st.session_state.current_section = 'Admin_Interviewers'
            elif selected_admin_nav == "📋 Draft Management":
                st.session_state.current_section = 'Draft_Dashboard'
            elif selected_admin_nav == "📈 Analytics & Reports":
                st.session_state.current_section = 'Admin_Analytics'
            elif selected_admin_nav == "⚙️ System Settings":
                st.session_state.current_section = 'Admin_Settings'
        
        # Quick draft access for interviewers
        if st.session_state.interviewer_logged_in:
            display_draft_quick_access()
        
        # Logout button
        st.markdown("---")
        if st.button("🚪 Logout", use_container_width=True):
            st.session_state.logged_in = False
            st.session_state.current_user = None
            st.session_state.user_role = None
            st.session_state.interviewer_logged_in = False
            st.session_state.admin_logged_in = False
            reset_interview()
            st.rerun()
    
    # Main content area
    if st.session_state.current_section == 'Dashboard':
        display_dashboard()
    elif st.session_state.current_section == 'A':
        display_section_a()
    elif st.session_state.current_section == 'B':
        enhanced_section_b()
    elif st.session_state.current_section == 'C':
        display_section_c()
    elif st.session_state.current_section == 'D':
        display_section_d()
    elif st.session_state.current_section == 'Draft_Dashboard':
        display_draft_dashboard()
    elif st.session_state.current_section == 'Admin_Dashboard':
        admin_dashboard()
    elif st.session_state.current_section == 'Admin_Interviewers':
        user_management_section()
    elif st.session_state.current_section == 'Admin_Analytics':
        analytics_main()
    elif st.session_state.current_section == 'Admin_Settings':
        database_tools_section()
    else:
        display_dashboard()

def display_dashboard():
    """Display main dashboard"""
    st.title("🏠 Compliance Survey Dashboard")
    
    if st.session_state.interviewer_logged_in:
        st.subheader("Welcome, Interviewer!")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Your Drafts", "5", "2 new")
        
        with col2:
            st.metric("Completed Surveys", "12", "3 this week")
        
        with col3:
            st.metric("Avg. Completion Time", "45 min", "-5 min")
        
        # Quick actions
        st.markdown("---")
        st.subheader("🚀 Quick Actions")
        
        action_col1, action_col2, action_col3 = st.columns(3)
        
        with action_col1:
            if st.button("🆕 Start New Interview", use_container_width=True):
                reset_interview()
                st.session_state.current_section = 'A'
                st.rerun()
        
        with action_col2:
            if st.button("📋 Manage Drafts", use_container_width=True):
                st.session_state.current_section = 'Draft_Dashboard'
                st.rerun()
        
        with action_col3:
            if st.button("📊 View Progress", use_container_width=True):
                st.session_state.current_section = 'Admin_Analytics'
                st.rerun()
    
    elif st.session_state.admin_logged_in:
        st.subheader("Admin Overview")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Total Surveys", "156", "12 new")
        
        with col2:
            st.metric("Active Interviewers", "6", "1 new")
        
        with col3:
            st.metric("Completion Rate", "78%", "5%")
        
        with col4:
            st.metric("Avg. Compliance Cost", "12.5%", "-2.3%")
        
        # Recent activity
        st.markdown("---")
        st.subheader("📈 Recent Activity")
        
        # Placeholder for recent activity chart
        st.info("📊 Analytics dashboard will show recent survey activity and trends")

# Admin Navigation Functions
def admin_dashboard():
    """Admin dashboard"""
    st.title("🔧 Admin Dashboard")
    st.subheader("Database Management & Analytics")
    
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
    
    st.header("💾 Data Management")
    
    tab1, tab2, tab3 = st.tabs(["All Interviews", "Search & Filter", "Data Export"])
    
    with tab1:
        display_all_interviews()
    
    with tab2:
        search_and_filter_interviews()
    
    with tab3:
        data_export_section()

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
        
        filtered_df = interviews_df.copy()
        if sector_filter:
            filtered_df = filtered_df[filtered_df['primary_sector'].isin(sector_filter)]
        if district_filter:
            filtered_df = filtered_df[filtered_df['district'].isin(district_filter)]
        if status_filter:
            filtered_df = filtered_df[filtered_df['status'].isin(status_filter)]
        
        st.write(f"**Filtered Results:** {len(filtered_df)} interviews")
        st.dataframe(filtered_df, use_container_width=True)
        
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
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                interviews_df.to_excel(writer, sheet_name='Interviews', index=False)
                
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
    """User management section"""
    st.header("👥 User Management")
    
    tab1, tab2 = st.tabs(["User Statistics", "Session Logs"])
    
    with tab1:
        st.subheader("User Statistics")
        
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
            st.success("Database cache refreshed!")
            log_admin_action(st.session_state.current_user, "refresh_cache")
        
        if st.button("📊 Update Statistics", use_container_width=True, key="update_stats_btn"):
            st.rerun()
    
    with col2:
        if st.button("🗑️ Clear All Drafts", use_container_width=True, key="clear_drafts_btn"):
            if st.checkbox("I understand this will delete all draft interviews", key="confirm_clear_drafts"):
                if st.button("Confirm Delete All Drafts", use_container_width=True, key="confirm_delete_drafts_btn"):
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
        
        if interview['reform_priorities']:
            st.write("**Reform Priorities**")
            reforms = json.loads(interview['reform_priorities'])
            for i, reform in enumerate(reforms, 1):
                st.write(f"{i}. {reform}")

# Run the application
if __name__ == "__main__":
    main()
