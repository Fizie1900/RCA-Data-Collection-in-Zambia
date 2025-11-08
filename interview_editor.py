# interview_editor.py
import streamlit as st
import pandas as pd
import sqlite3
import json
from datetime import datetime

class InterviewEditor:
    def __init__(self):
        self.conn = sqlite3.connect('compliance_survey.db', check_same_thread=False)
    
    def ensure_table_exists(self):
        """Ensure the responses table exists"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='responses'")
            if not cursor.fetchone():
                st.error("❌ Database table 'responses' does not exist. Please initialize the database first.")
                return False
            return True
        except Exception as e:
            st.error(f"Error checking table existence: {str(e)}")
            return False
    
    def get_submitted_interviews(self):
        """Get all submitted interviews for editing"""
        try:
            if not self.ensure_table_exists():
                return pd.DataFrame()
                
            query = """
            SELECT 
                interview_id, business_name, district, primary_sector, 
                business_size, status, submission_date, last_modified,
                created_by
            FROM responses 
            WHERE status = 'submitted'
            ORDER BY submission_date DESC
            """
            df = pd.read_sql(query, self.conn)
            return df
        except Exception as e:
            st.error(f"Error loading interviews: {str(e)}")
            return pd.DataFrame()
    
    def get_interview_details(self, interview_id):
        """Get complete interview details"""
        try:
            if not self.ensure_table_exists():
                return None
                
            query = "SELECT * FROM responses WHERE interview_id = ?"
            df = pd.read_sql(query, self.conn, params=(interview_id,))
            if not df.empty:
                return df.iloc[0].to_dict()
            return None
        except Exception as e:
            st.error(f"Error loading interview details: {str(e)}")
            return None
    
    def update_interview(self, interview_id, updates):
        """Update interview data"""
        try:
            if not self.ensure_table_exists():
                return False
                
            cursor = self.conn.cursor()
            
            set_clause = ", ".join([f"{key} = ?" for key in updates.keys()])
            values = list(updates.values()) + [datetime.now().isoformat(), interview_id]
            
            query = f"UPDATE responses SET {set_clause}, last_modified = ? WHERE interview_id = ?"
            
            cursor.execute(query, values)
            self.conn.commit()
            
            self.log_edit_action(st.session_state.current_user, interview_id, updates)
            
            return True
        except Exception as e:
            st.error(f"Error updating interview: {str(e)}")
            return False
    
    def log_edit_action(self, username, interview_id, changes):
        """Log edit actions for audit trail"""
        try:
            cursor = self.conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS edit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT,
                    interview_id TEXT,
                    action TEXT,
                    changes TEXT,
                    timestamp TIMESTAMP
                )
            ''')
            
            cursor.execute('''
                INSERT INTO edit_logs (username, interview_id, action, changes, timestamp)
                VALUES (?, ?, ?, ?, ?)
            ''', (username, interview_id, 'edit', json.dumps(changes), datetime.now().isoformat()))
            
            self.conn.commit()
        except Exception as e:
            st.error(f"Error logging edit action: {str(e)}")
    
    def revert_to_draft(self, interview_id):
        """Revert a submitted interview back to draft status"""
        try:
            if not self.ensure_table_exists():
                return False
                
            cursor = self.conn.cursor()
            cursor.execute('''
                UPDATE responses 
                SET status = 'draft', last_modified = ?
                WHERE interview_id = ?
            ''', (datetime.now().isoformat(), interview_id))
            
            self.conn.commit()
            
            self.log_edit_action(st.session_state.current_user, interview_id, {'status': 'reverted_to_draft'})
            
            return True
        except Exception as e:
            st.error(f"Error reverting to draft: {str(e)}")
            return False

def display_interview_selector(editor):
    """Display interface to select interviews for editing"""
    st.header("📝 Interview Editor")
    
    interviews_df = editor.get_submitted_interviews()
    
    if interviews_df.empty:
        st.info("No submitted interviews available for editing.")
        return None
    
    st.subheader("Select Interview to Edit")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        search_term = st.text_input("🔍 Search by business name:", placeholder="Enter business name...")
    
    with col2:
        sector_filter = st.selectbox("Filter by sector:", 
                                   ['All'] + list(interviews_df['primary_sector'].unique()))
    
    with col3:
        district_filter = st.selectbox("Filter by district:",
                                     ['All'] + list(interviews_df['district'].unique()))
    
    filtered_df = interviews_df.copy()
    if search_term:
        filtered_df = filtered_df[filtered_df['business_name'].str.contains(search_term, case=False, na=False)]
    if sector_filter != 'All':
        filtered_df = filtered_df[filtered_df['primary_sector'] == sector_filter]
    if district_filter != 'All':
        filtered_df = filtered_df[filtered_df['district'] == district_filter]
    
    if filtered_df.empty:
        st.warning("No interviews match the selected filters.")
        return None
    
    st.write(f"**Found {len(filtered_df)} interviews:**")
    
    selected_index = st.selectbox(
        "Select interview to edit:",
        range(len(filtered_df)),
        format_func=lambda x: f"{filtered_df.iloc[x]['business_name']} - {filtered_df.iloc[x]['district']} - {filtered_df.iloc[x]['primary_sector']}"
    )
    
    if st.button("📝 Edit Selected Interview", use_container_width=True):
        selected_interview = filtered_df.iloc[selected_index]
        return selected_interview['interview_id']
    
    return None

def display_interview_editor(editor, interview_id):
    """Display the main interview editor interface"""
    interview_data = editor.get_interview_details(interview_id)
    
    if not interview_data:
        st.error("Interview not found!")
        return
    
    st.header(f"✏️ Editing: {interview_data['business_name']}")
    
    display_edit_history(editor, interview_id)
    
    edit_option = st.radio(
        "Edit Options:",
        ["Basic Information", "Business Details", "Compliance Procedures", "Advanced Options"],
        horizontal=True
    )
    
    if edit_option == "Basic Information":
        edit_basic_information(editor, interview_id, interview_data)
    elif edit_option == "Business Details":
        edit_business_details(editor, interview_id, interview_data)
    elif edit_option == "Compliance Procedures":
        edit_compliance_procedures(editor, interview_id, interview_data)
    elif edit_option == "Advanced Options":
        display_advanced_options(editor, interview_id, interview_data)

def display_edit_history(editor, interview_id):
    """Display edit history for an interview"""
    try:
        cursor = editor.conn.cursor()
        cursor.execute('''
            SELECT username, action, changes, timestamp 
            FROM edit_logs 
            WHERE interview_id = ? 
            ORDER BY timestamp DESC
            LIMIT 5
        ''', (interview_id,))
        
        history = cursor.fetchall()
        
        if history:
            with st.expander("📋 Edit History (Last 5 actions)"):
                for record in history:
                    st.write(f"**{record[0]}** - {record[1]} - {record[3]}")
                    if record[2] and record[2] != 'null':
                        try:
                            changes = json.loads(record[2])
                            for key, value in changes.items():
                                st.write(f"  - {key}: {value}")
                        except:
                            st.write(f"  - Changes: {record[2]}")
    except:
        pass

def edit_basic_information(editor, interview_id, interview_data):
    """Edit basic interview information"""
    st.subheader("🏢 Basic Business Information")
    
    with st.form("basic_info_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            business_name = st.text_input(
                "Business Name *",
                value=interview_data.get('business_name', ''),
                key="edit_business_name"
            )
            
            district = st.selectbox(
                "District *",
                ["Lusaka", "Kitwe", "Kasama", "Ndola", "Livingstone", "Other (Please specify)"],
                index=["Lusaka", "Kitwe", "Kasama", "Ndola", "Livingstone", "Other (Please specify)"].index(
                    interview_data.get('district', 'Lusaka')
                ) if interview_data.get('district') in ["Lusaka", "Kitwe", "Kasama", "Ndola", "Livingstone", "Other (Please specify)"] else 0,
                key="edit_district"
            )
            
            physical_address = st.text_area(
                "Physical Address",
                value=interview_data.get('physical_address', ''),
                key="edit_physical_address"
            )
        
        with col2:
            contact_person = st.text_input(
                "Contact Person *",
                value=interview_data.get('contact_person', ''),
                key="edit_contact_person"
            )
            
            email = st.text_input(
                "Email Address",
                value=interview_data.get('email', ''),
                key="edit_email"
            )
            
            phone = st.text_input(
                "Phone Number",
                value=interview_data.get('phone', ''),
                key="edit_phone"
            )
        
        if st.form_submit_button("💾 Save Basic Information", use_container_width=True):
            updates = {
                'business_name': business_name,
                'district': district,
                'physical_address': physical_address,
                'contact_person': contact_person,
                'email': email,
                'phone': phone
            }
            
            if editor.update_interview(interview_id, updates):
                st.success("✅ Basic information updated successfully!")
                st.rerun()

def edit_business_details(editor, interview_id, interview_data):
    """Edit business classification and details"""
    st.subheader("📊 Business Classification")
    
    with st.form("business_details_form"):
        col1, col2 = st.columns(2)
        
        with col1:
            primary_sector = st.radio(
                "Primary Sector *",
                ["Agribusiness", "Construction"],
                index=0 if interview_data.get('primary_sector') == "Agribusiness" else 1,
                key="edit_primary_sector"
            )
            
            legal_status = st.selectbox(
                "Legal Status *",
                ["Sole Proprietor", "Partnership", "Limited Liability Company", "Public Limited Company", "Other"],
                index=["Sole Proprietor", "Partnership", "Limited Liability Company", "Public Limited Company", "Other"].index(
                    interview_data.get('legal_status', 'Sole Proprietor')
                ) if interview_data.get('legal_status') in ["Sole Proprietor", "Partnership", "Limited Liability Company", "Public Limited Company", "Other"] else 0,
                key="edit_legal_status"
            )
        
        with col2:
            business_size = st.selectbox(
                "Business Size *",
                ["Micro (1-9)", "Small (10-49)", "Medium (50-249)", "Large (250+)"],
                index=["Micro (1-9)", "Small (10-49)", "Medium (50-249)", "Large (250+)"].index(
                    interview_data.get('business_size', 'Micro (1-9)')
                ) if interview_data.get('business_size') in ["Micro (1-9)", "Small (10-49)", "Medium (50-249)", "Large (250+)"] else 0,
                key="edit_business_size"
            )
            
            ownership_structure = st.selectbox(
                "Ownership Structure *",
                ["100% Zambian-owned", "Partially Foreign-owned", "Majority/Fully Foreign-owned", "Other"],
                index=["100% Zambian-owned", "Partially Foreign-owned", "Majority/Fully Foreign-owned", "Other"].index(
                    interview_data.get('ownership_structure', '100% Zambian-owned')
                ) if interview_data.get('ownership_structure') in ["100% Zambian-owned", "Partially Foreign-owned", "Majority/Fully Foreign-owned", "Other"] else 0,
                key="edit_ownership"
            )
            
            gender_owner = st.radio(
                "Gender of Majority Owner/CEO *",
                ["Male", "Female", "Joint (M/F)"],
                index=["Male", "Female", "Joint (M/F)"].index(
                    interview_data.get('gender_owner', 'Male')
                ) if interview_data.get('gender_owner') in ["Male", "Female", "Joint (M/F)"] else 0,
                key="edit_gender_owner"
            )
        
        st.subheader("Business Background")
        business_activities = st.text_area(
            "Business Activities Description *",
            value=interview_data.get('business_activities', ''),
            height=100,
            key="edit_business_activities"
        )
        
        col1, col2, col3 = st.columns(3)
        with col1:
            year_established = st.number_input(
                "Year of Establishment",
                min_value=1900,
                max_value=2024,
                value=int(interview_data.get('year_established', 2020)),
                key="edit_year_established"
            )
        
        with col2:
            employees_fulltime = st.number_input(
                "Full-time Employees",
                min_value=0,
                value=int(interview_data.get('employees_fulltime', 0)),
                key="edit_employees_fulltime"
            )
        
        with col3:
            employees_parttime = st.number_input(
                "Part-time Employees",
                min_value=0,
                value=int(interview_data.get('employees_parttime', 0)),
                key="edit_employees_parttime"
            )
        
        if st.form_submit_button("💾 Save Business Details", use_container_width=True):
            updates = {
                'primary_sector': primary_sector,
                'legal_status': legal_status,
                'business_size': business_size,
                'ownership_structure': ownership_structure,
                'gender_owner': gender_owner,
                'business_activities': business_activities,
                'year_established': year_established,
                'employees_fulltime': employees_fulltime,
                'employees_parttime': employees_parttime
            }
            
            if editor.update_interview(interview_id, updates):
                st.success("✅ Business details updated successfully!")
                st.rerun()

def edit_compliance_procedures(editor, interview_id, interview_data):
    """Edit compliance procedures data"""
    st.subheader("📑 Compliance Procedures")
    
    procedures_json = interview_data.get('procedure_data')
    procedures = []
    
    if procedures_json and procedures_json != 'null' and procedures_json != '[]':
        try:
            procedures = json.loads(procedures_json)
        except:
            procedures = []
    
    if not procedures:
        st.info("No compliance procedures data found for this interview.")
        return
    
    for i, procedure in enumerate(procedures):
        with st.expander(f"🔧 {procedure.get('procedure', 'Unknown Procedure')} - {procedure.get('authority', 'Unknown Authority')}", expanded=False):
            edit_single_procedure(editor, interview_id, procedures, i, procedure)
    
    st.subheader("Quick Actions")
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("🔄 Recalculate Totals", use_container_width=True):
            recalculate_totals(editor, interview_id, procedures)
    
    with col2:
        if st.button("📊 Validate Data", use_container_width=True):
            validate_procedures_data(procedures)

def edit_single_procedure(editor, interview_id, procedures, index, procedure):
    """Edit a single procedure"""
    with st.form(f"edit_procedure_{index}"):
        col1, col2 = st.columns(2)
        
        with col1:
            procedure_name = st.text_input(
                "Procedure Name",
                value=procedure.get('procedure', ''),
                key=f"proc_name_{index}"
            )
            
            authority = st.text_input(
                "Regulatory Authority",
                value=procedure.get('authority', ''),
                key=f"authority_{index}"
            )
            
            status = st.selectbox(
                "Status",
                ["Not Started", "In Progress", "Completed", "Delayed", "Rejected"],
                index=["Not Started", "In Progress", "Completed", "Delayed", "Rejected"].index(
                    procedure.get('status', 'Completed')
                ),
                key=f"status_{index}"
            )
        
        with col2:
            official_fees = st.number_input(
                "Official Fees (ZMW)",
                min_value=0.0,
                value=float(procedure.get('official_fees', 0)),
                key=f"official_fees_{index}"
            )
            
            total_days = st.number_input(
                "Total Days",
                min_value=0,
                value=int(procedure.get('total_days', 0)),
                key=f"total_days_{index}"
            )
            
            complexity = st.slider(
                "Complexity (1-5)",
                min_value=1,
                max_value=5,
                value=int(procedure.get('complexity', 3)),
                key=f"complexity_{index}"
            )
        
        if st.form_submit_button(f"💾 Update Procedure {index + 1}", use_container_width=True):
            procedures[index].update({
                'procedure': procedure_name,
                'authority': authority,
                'status': status,
                'official_fees': official_fees,
                'total_days': total_days,
                'complexity': complexity
            })
            
            updates = {
                'procedure_data': json.dumps(procedures)
            }
            
            if editor.update_interview(interview_id, updates):
                st.success(f"✅ Procedure {index + 1} updated successfully!")
                st.rerun()

def recalculate_totals(editor, interview_id, procedures):
    """Recalculate total compliance costs and times"""
    try:
        total_cost = sum(proc.get('official_fees', 0) + proc.get('unofficial_payments', 0) for proc in procedures)
        total_time = sum(proc.get('total_days', 0) for proc in procedures)
        
        risk_score = min((total_cost / 100000 + total_time / 365) * 10, 10)
        
        updates = {
            'total_compliance_cost': total_cost,
            'total_compliance_time': total_time,
            'risk_score': risk_score
        }
        
        if editor.update_interview(interview_id, updates):
            st.success("✅ Totals recalculated successfully!")
            st.rerun()
    except Exception as e:
        st.error(f"Error recalculating totals: {str(e)}")

def validate_procedures_data(procedures):
    """Validate procedures data for common errors"""
    errors = []
    warnings = []
    
    for i, proc in enumerate(procedures):
        if not proc.get('procedure'):
            errors.append(f"Procedure {i+1}: Missing procedure name")
        if not proc.get('authority'):
            errors.append(f"Procedure {i+1}: Missing regulatory authority")
        
        if proc.get('official_fees', 0) < 0:
            warnings.append(f"Procedure {i+1}: Official fees cannot be negative")
        if proc.get('total_days', 0) < 0:
            warnings.append(f"Procedure {i+1}: Total days cannot be negative")
    
    if errors:
        st.error("**Validation Errors:**")
        for error in errors:
            st.write(f"❌ {error}")
    
    if warnings:
        st.warning("**Validation Warnings:**")
        for warning in warnings:
            st.write(f"⚠️ {warning}")
    
    if not errors and not warnings:
        st.success("✅ All procedures data is valid!")

def display_advanced_options(editor, interview_id, interview_data):
    """Display advanced editing options"""
    st.subheader("⚙️ Advanced Options")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Data Management**")
        
        if st.button("📋 Export Interview Data", use_container_width=True):
            export_interview_data(interview_data)
        
        if st.button("🔄 Refresh from Database", use_container_width=True):
            st.rerun()
    
    with col2:
        st.write("**Status Management**")
        
        if st.button("📝 Revert to Draft", use_container_width=True):
            if editor.revert_to_draft(interview_id):
                st.success("✅ Interview reverted to draft status!")
                st.rerun()
        
        if st.button("🗑️ Delete Interview", use_container_width=True):
            st.error("This action cannot be undone!")
            if st.checkbox("I understand this will permanently delete the interview"):
                if st.button("Confirm Permanent Deletion"):
                    delete_interview(editor, interview_id)

def export_interview_data(interview_data):
    """Export interview data as JSON"""
    try:
        export_data = {k: v for k, v in interview_data.items() if v is not None}
        
        json_data = json.dumps(export_data, indent=2, default=str)
        
        st.download_button(
            label="📥 Download Interview Data (JSON)",
            data=json_data,
            file_name=f"interview_{interview_data['interview_id']}_{datetime.now().strftime('%Y%m%d')}.json",
            mime="application/json"
        )
    except Exception as e:
        st.error(f"Error exporting data: {str(e)}")

def delete_interview(editor, interview_id):
    """Delete an interview (admin only)"""
    try:
        cursor = editor.conn.cursor()
        cursor.execute("DELETE FROM responses WHERE interview_id = ?", (interview_id,))
        editor.conn.commit()
        
        editor.log_edit_action(st.session_state.current_user, interview_id, {'action': 'permanent_deletion'})
        
        st.success("✅ Interview deleted successfully!")
        st.rerun()
    except Exception as e:
        st.error(f"Error deleting interview: {str(e)}")

def interview_editor_main():
    """Main function for the interview editor"""
    if not st.session_state.get('admin_logged_in', False):
        st.error("🔒 Administrator access required to edit interviews.")
        return
    
    editor = InterviewEditor()
    
    if 'editing_interview_id' in st.session_state:
        display_interview_editor(editor, st.session_state.editing_interview_id)
        
        if st.button("← Back to Interview List", key="back_to_list"):
            del st.session_state.editing_interview_id
            st.rerun()
    else:
        selected_interview_id = display_interview_selector(editor)
        if selected_interview_id:
            st.session_state.editing_interview_id = selected_interview_id
            st.rerun()

def run_interview_editor():
    """Run interview editor as standalone app"""
    st.set_page_config(
        page_title="Interview Editor - Compliance System",
        page_icon="✏️",
        layout="wide"
    )
    st.title("✏️ Interview Editor")
    interview_editor_main()

if __name__ == "__main__":
    run_interview_editor()
