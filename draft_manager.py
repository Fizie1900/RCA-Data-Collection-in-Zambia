# draft_manager.py
import streamlit as st
import pandas as pd
import sqlite3
from datetime import datetime
import json

class DraftManager:
    def __init__(self):
        self.conn = sqlite3.connect('compliance_survey.db', check_same_thread=False)
    
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
            df = pd.read_sql(query, self.conn, params=(username,))
            return df
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
            df = pd.read_sql(query, self.conn)
            return df
        except Exception as e:
            st.error(f"Error loading drafts: {str(e)}")
            return pd.DataFrame()
    
    def load_draft(self, interview_id):
        """Load a specific draft by interview ID"""
        try:
            query = "SELECT * FROM responses WHERE interview_id = ?"
            df = pd.read_sql(query, self.conn, params=(interview_id,))
            if not df.empty:
                return df.iloc[0].to_dict()
            return None
        except Exception as e:
            st.error(f"Error loading draft: {str(e)}")
            return None
    
    def update_draft_progress(self, interview_id, current_section, progress_percentage):
        """Update draft progress and current section"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                UPDATE responses 
                SET current_section = ?, draft_progress = ?, last_modified = ?
                WHERE interview_id = ?
            ''', (current_section, progress_percentage, datetime.now().isoformat(), interview_id))
            self.conn.commit()
            return True
        except Exception as e:
            st.error(f"Error updating draft progress: {str(e)}")
            return False
    
    def delete_draft(self, interview_id):
        """Delete a draft interview"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM responses WHERE interview_id = ? AND status = 'draft'", (interview_id,))
            self.conn.commit()
            return True
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
    
    draft_manager = DraftManager()
    
    if st.session_state.get('admin_logged_in', False):
        drafts_df = draft_manager.get_all_drafts()
        user_type = "All Users"
    else:
        drafts_df = draft_manager.get_user_drafts(st.session_state.current_user)
        user_type = "Your"
    
    if not drafts_df.empty:
        st.subheader(f"{user_type} Draft Interviews ({len(drafts_df)})")
        
        for index, draft in drafts_df.iterrows():
            display_draft_card(draft_manager, draft, index)
    else:
        st.info("💡 No draft interviews found. Start a new interview to create drafts!")
    
    st.sidebar.markdown("---")
    st.sidebar.subheader("🚀 Quick Actions")
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🆕 New Interview", use_container_width=True, key="new_interview_btn"):
            from app import reset_interview
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
        
        procedures_json = draft_data.get('procedure_data')
        if procedures_json and procedures_json != 'null' and procedures_json != '[]':
            try:
                st.session_state.procedures_list = json.loads(procedures_json)
            except:
                st.session_state.procedures_list = []
        else:
            st.session_state.procedures_list = []
        
        isic_json = draft_data.get('isic_codes')
        if isic_json and isic_json != 'null' and isic_json != '[]':
            try:
                st.session_state.selected_isic_codes = json.loads(isic_json)
            except:
                st.session_state.selected_isic_codes = []
        else:
            st.session_state.selected_isic_codes = []
        
        st.session_state.business_activities_text = draft_data.get('business_activities', '')
        
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
            
            for index, draft in drafts_df.head(3).iterrows():
                business_name = draft['business_name'] or 'Unnamed Business'
                progress = draft['draft_progress'] or 0
                
                if st.sidebar.button(
                    f"➡️ {business_name[:20]}... ({progress}%)", 
                    key=f"sidebar_draft_{index}",
                    use_container_width=True
                ):
                    load_draft_into_session(draft_manager, draft['interview_id'])
                    st.rerun()
            
            if len(drafts_df) > 3:
                if st.sidebar.button("📋 View All Drafts", use_container_width=True):
                    st.session_state.current_section = 'Draft_Dashboard'
                    st.rerun()

def auto_save_draft():
    """Auto-save current form state as draft"""
    if (st.session_state.get('form_data') and 
        st.session_state.get('current_interview_id') and
        st.session_state.get('interviewer_logged_in', False)):
        
        draft_manager = DraftManager()
        progress = draft_manager.calculate_progress(
            st.session_state.form_data, 
            st.session_state.current_section
        )
        
        draft_manager.update_draft_progress(
            st.session_state.current_interview_id,
            st.session_state.current_section,
            progress
        )
        
        return True
    return False

if __name__ == "__main__":
    display_draft_dashboard()
