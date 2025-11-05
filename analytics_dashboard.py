# analytics_dashboard.py
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import json
import sqlite3

class ComplianceAnalytics:
    def __init__(self):
        self.conn = sqlite3.connect('compliance_survey.db', check_same_thread=False)
        
    def get_analytics_data(self):
        """Get comprehensive data for analytics"""
        try:
            query = """
            SELECT 
                r.*,
                json_extract(r.procedure_data, '$') as procedures_json
            FROM responses r
            WHERE r.status = 'submitted'
            """
            df = pd.read_sql(query, self.conn)
            
            # Parse procedures data
            procedures_data = []
            for _, row in df.iterrows():
                if row['procedures_json'] and row['procedures_json'] != 'null' and row['procedures_json'] != '[]':
                    try:
                        procedures = json.loads(row['procedures_json'])
                        for proc in procedures:
                            proc['interview_id'] = row['interview_id']
                            proc['business_name'] = row['business_name']
                            proc['district'] = row['district']
                            proc['primary_sector'] = row['primary_sector']
                            proc['business_size'] = row['business_size']
                            procedures_data.append(proc)
                    except Exception as e:
                        continue
            
            procedures_df = pd.DataFrame(procedures_data) if procedures_data else pd.DataFrame()
            
            return df, procedures_df
        except Exception as e:
            st.error(f"Error loading analytics data: {str(e)}")
            return pd.DataFrame(), pd.DataFrame()
    
    def create_compliance_matrix(self, procedures_df):
        """Create compliance cost and time matrix"""
        if procedures_df.empty:
            return pd.DataFrame()
        
        matrix_data = []
        
        # Group by procedure type and calculate metrics
        procedure_groups = procedures_df.groupby('procedure')
        
        for procedure, group in procedure_groups:
            matrix_data.append({
                'Procedure': procedure,
                'Authority': group['authority'].iloc[0] if len(group) > 0 else 'Unknown',
                'Avg_Official_Cost': group['official_fees'].mean(),
                'Avg_Unofficial_Cost': group.get('unofficial_payments', pd.Series([0])).mean(),
                'Avg_Total_Cost': (group['official_fees'] + group.get('unofficial_payments', pd.Series([0]))).mean(),
                'Avg_Time_Days': group['total_days'].mean(),
                'Avg_Complexity': group['complexity'].mean(),
                'Frequency': len(group),
                'Avg_Prep_Days': group['prep_days'].mean(),
                'Avg_Wait_Days': group['wait_days'].mean(),
                'Most_Common_Mode': group['application_mode'].mode().iloc[0] if not group['application_mode'].mode().empty else 'Unknown'
            })
        
        matrix_df = pd.DataFrame(matrix_data)
        return matrix_df.sort_values('Avg_Total_Cost', ascending=False)
    
    def create_sector_analysis(self, df, procedures_df):
        """Create sector-wise analysis"""
        if df.empty or procedures_df.empty:
            return pd.DataFrame()
        
        sector_data = []
        
        for sector in df['primary_sector'].unique():
            sector_businesses = df[df['primary_sector'] == sector]
            sector_procedures = procedures_df[procedures_df['primary_sector'] == sector]
            
            if len(sector_businesses) == 0:
                continue
                
            sector_data.append({
                'Sector': sector,
                'Business_Count': len(sector_businesses),
                'Avg_Total_Cost': sector_businesses['total_compliance_cost'].mean(),
                'Avg_Total_Time': sector_businesses['total_compliance_time'].mean(),
                'Avg_Risk_Score': sector_businesses['risk_score'].mean(),
                'Procedures_Per_Business': len(sector_procedures) / len(sector_businesses) if len(sector_businesses) > 0 else 0,
                'Most_Common_Procedure': sector_procedures['procedure'].mode().iloc[0] if not sector_procedures['procedure'].mode().empty else 'None',
                'Highest_Cost_Procedure': sector_procedures.loc[sector_procedures['official_fees'].idxmax()]['procedure'] if not sector_procedures.empty else 'None'
            })
        
        return pd.DataFrame(sector_data)

def display_overview_metrics(df, procedures_df):
    """Display overview metrics and visualizations"""
    st.header("📈 Compliance Overview")
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_businesses = len(df)
        st.metric("Total Businesses", total_businesses)
    
    with col2:
        avg_cost = df['total_compliance_cost'].mean() if not df.empty else 0
        st.metric("Average Compliance Cost", f"ZMW {avg_cost:,.0f}")
    
    with col3:
        avg_time = df['total_compliance_time'].mean() if not df.empty else 0
        st.metric("Average Compliance Time", f"{avg_time:.0f} days")
    
    with col4:
        avg_risk = df['risk_score'].mean() if not df.empty else 0
        st.metric("Average Risk Score", f"{avg_risk:.1f}/10")
    
    if not df.empty:
        # Cost distribution
        col1, col2 = st.columns(2)
        
        with col1:
            fig_cost = px.histogram(df, x='total_compliance_cost', 
                                   title="Distribution of Total Compliance Costs",
                                   labels={'total_compliance_cost': 'Total Cost (ZMW)'})
            fig_cost.update_layout(showlegend=False)
            st.plotly_chart(fig_cost, use_container_width=True)
        
        with col2:
            fig_time = px.histogram(df, x='total_compliance_time',
                                   title="Distribution of Compliance Time",
                                   labels={'total_compliance_time': 'Total Time (Days)'})
            fig_time.update_layout(showlegend=False)
            st.plotly_chart(fig_time, use_container_width=True)
        
        # Top procedures by cost
        if not procedures_df.empty:
            st.subheader("🏆 Most Expensive Procedures")
            
            top_procedures = procedures_df.groupby('procedure').agg({
                'official_fees': 'mean',
                'total_days': 'mean',
                'complexity': 'mean'
            }).round(0).sort_values('official_fees', ascending=False).head(10)
            
            top_procedures.columns = ['Avg Cost (ZMW)', 'Avg Time (Days)', 'Avg Complexity']
            st.dataframe(top_procedures, use_container_width=True)

def display_cost_matrix(analytics, procedures_df):
    """Display compliance cost matrix"""
    st.header("💰 Compliance Cost Matrix")
    
    matrix_df = analytics.create_compliance_matrix(procedures_df)
    
    if matrix_df.empty:
        st.info("No procedure data available for matrix analysis.")
        return
    
    # Interactive matrix visualization
    col1, col2 = st.columns(2)
    
    with col1:
        # Cost vs Time scatter plot
        fig_scatter = px.scatter(matrix_df, 
                               x='Avg_Time_Days', 
                               y='Avg_Total_Cost',
                               size='Frequency',
                               color='Avg_Complexity',
                               hover_name='Procedure',
                               title="Cost vs Time Analysis",
                               labels={
                                   'Avg_Time_Days': 'Average Time (Days)',
                                   'Avg_Total_Cost': 'Average Total Cost (ZMW)',
                                   'Frequency': 'Frequency',
                                   'Avg_Complexity': 'Complexity'
                               })
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    with col2:
        # Cost breakdown by procedure
        top_10 = matrix_df.head(10)
        fig_bar = px.bar(top_10,
                        x='Procedure',
                        y=['Avg_Official_Cost', 'Avg_Unofficial_Cost'],
                        title="Top 10 Procedures - Cost Breakdown",
                        labels={'value': 'Cost (ZMW)', 'variable': 'Cost Type'})
        fig_bar.update_layout(xaxis_tickangle=-45)
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # Detailed matrix table
    st.subheader("📊 Detailed Compliance Matrix")
    
    display_matrix = matrix_df[['Procedure', 'Authority', 'Avg_Total_Cost', 'Avg_Time_Days', 
                              'Avg_Complexity', 'Frequency', 'Most_Common_Mode']].copy()
    display_matrix['Avg_Total_Cost'] = display_matrix['Avg_Total_Cost'].round(0)
    display_matrix['Avg_Time_Days'] = display_matrix['Avg_Time_Days'].round(1)
    display_matrix['Avg_Complexity'] = display_matrix['Avg_Complexity'].round(1)
    
    display_matrix.columns = ['Procedure', 'Authority', 'Avg Cost (ZMW)', 'Avg Time (Days)', 
                            'Complexity (1-5)', 'Frequency', 'Common Mode']
    
    st.dataframe(display_matrix, use_container_width=True)
    
    # Download matrix
    csv = matrix_df.to_csv(index=False)
    st.download_button(
        label="📥 Download Cost Matrix (CSV)",
        data=csv,
        file_name=f"compliance_cost_matrix_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

def display_sector_analysis(analytics, df, procedures_df):
    """Display sector-wise analysis"""
    st.header("🏢 Sector Analysis")
    
    sector_df = analytics.create_sector_analysis(df, procedures_df)
    
    if sector_df.empty:
        st.info("No sector data available for analysis.")
        return
    
    # Sector comparison
    col1, col2 = st.columns(2)
    
    with col1:
        fig_sector_cost = px.bar(sector_df, 
                                x='Sector', 
                                y='Avg_Total_Cost',
                                title="Average Compliance Cost by Sector",
                                labels={'Avg_Total_Cost': 'Average Cost (ZMW)'})
        st.plotly_chart(fig_sector_cost, use_container_width=True)
    
    with col2:
        fig_sector_time = px.bar(sector_df,
                                x='Sector',
                                y='Avg_Total_Time',
                                title="Average Compliance Time by Sector",
                                labels={'Avg_Total_Time': 'Average Time (Days)'})
        st.plotly_chart(fig_sector_time, use_container_width=True)
    
    # Risk and efficiency analysis
    col1, col2 = st.columns(2)
    
    with col1:
        fig_risk = px.scatter(sector_df,
                            x='Avg_Total_Cost',
                            y='Avg_Risk_Score',
                            size='Business_Count',
                            color='Sector',
                            title="Cost vs Risk by Sector",
                            labels={
                                'Avg_Total_Cost': 'Average Cost (ZMW)',
                                'Avg_Risk_Score': 'Average Risk Score',
                                'Business_Count': 'Number of Businesses'
                            })
        st.plotly_chart(fig_risk, use_container_width=True)
    
    with col2:
        fig_efficiency = px.bar(sector_df,
                              x='Sector',
                              y='Procedures_Per_Business',
                              title="Procedures per Business by Sector",
                              labels={'Procedures_Per_Business': 'Procedures per Business'})
        st.plotly_chart(fig_efficiency, use_container_width=True)
    
    # Sector details table
    st.subheader("📈 Sector Performance Metrics")
    
    display_sector = sector_df[['Sector', 'Business_Count', 'Avg_Total_Cost', 'Avg_Total_Time',
                              'Avg_Risk_Score', 'Procedures_Per_Business', 'Most_Common_Procedure']].copy()
    display_sector['Avg_Total_Cost'] = display_sector['Avg_Total_Cost'].round(0)
    display_sector['Avg_Total_Time'] = display_sector['Avg_Total_Time'].round(1)
    display_sector['Avg_Risk_Score'] = display_sector['Avg_Risk_Score'].round(1)
    display_sector['Procedures_Per_Business'] = display_sector['Procedures_Per_Business'].round(1)
    
    display_sector.columns = ['Sector', 'Business Count', 'Avg Cost (ZMW)', 'Avg Time (Days)',
                            'Risk Score', 'Procedures/Business', 'Most Common Procedure']
    
    st.dataframe(display_sector, use_container_width=True)

def display_time_analysis(procedures_df):
    """Display time analysis visualizations"""
    st.header("⏱️ Time Analysis")
    
    if procedures_df.empty:
        st.info("No procedure data available for time analysis.")
        return
    
    # Time distribution analysis
    col1, col2 = st.columns(2)
    
    with col1:
        # Preparation vs Waiting time
        fig_time_breakdown = go.Figure()
        fig_time_breakdown.add_trace(go.Box(y=procedures_df['prep_days'], name='Preparation Time'))
        fig_time_breakdown.add_trace(go.Box(y=procedures_df['wait_days'], name='Waiting Time'))
        fig_time_breakdown.update_layout(title="Preparation vs Waiting Time Distribution")
        st.plotly_chart(fig_time_breakdown, use_container_width=True)
    
    with col2:
        # Time by application mode
        time_by_mode = procedures_df.groupby('application_mode').agg({
            'total_days': 'mean',
            'prep_days': 'mean',
            'wait_days': 'mean'
        }).reset_index()
        
        fig_mode_time = px.bar(time_by_mode, 
                              x='application_mode',
                              y=['prep_days', 'wait_days'],
                              title="Time Analysis by Application Mode",
                              labels={'value': 'Days', 'variable': 'Time Type'})
        st.plotly_chart(fig_mode_time, use_container_width=True)
    
    # Time trends
    st.subheader("📅 Time Efficiency Analysis")
    
    # Most time-consuming procedures
    time_consuming = procedures_df.groupby('procedure').agg({
        'total_days': ['mean', 'std', 'count']
    }).round(1)
    time_consuming.columns = ['Avg_Days', 'Std_Days', 'Count']
    time_consuming = time_consuming.sort_values('Avg_Days', ascending=False).head(15)
    
    fig_time_procedures = px.bar(time_consuming.reset_index(),
                                x='procedure',
                                y='Avg_Days',
                                error_y='Std_Days',
                                title="Most Time-Consuming Procedures",
                                labels={'Avg_Days': 'Average Days', 'procedure': 'Procedure'})
    fig_time_procedures.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig_time_procedures, use_container_width=True)

def display_procedure_details(procedures_df):
    """Display detailed procedure analysis"""
    st.header("📋 Procedure Details Analysis")
    
    if procedures_df.empty:
        st.info("No procedure data available for detailed analysis.")
        return
    
    # Procedure selector
    procedures = procedures_df['procedure'].unique()
    selected_procedure = st.selectbox("Select Procedure for Detailed Analysis", procedures)
    
    if selected_procedure:
        proc_data = procedures_df[procedures_df['procedure'] == selected_procedure]
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            avg_cost = proc_data['official_fees'].mean()
            st.metric("Average Official Cost", f"ZMW {avg_cost:,.0f}")
        
        with col2:
            avg_time = proc_data['total_days'].mean()
            st.metric("Average Time", f"{avg_time:.1f} days")
        
        with col3:
            avg_complexity = proc_data['complexity'].mean()
            st.metric("Average Complexity", f"{avg_complexity:.1f}/5")
        
        with col4:
            frequency = len(proc_data)
            st.metric("Frequency", frequency)
        
        # Cost distribution for selected procedure
        col1, col2 = st.columns(2)
        
        with col1:
            fig_cost_dist = px.histogram(proc_data, x='official_fees',
                                       title=f"Cost Distribution - {selected_procedure}",
                                       labels={'official_fees': 'Official Cost (ZMW)'})
            st.plotly_chart(fig_cost_dist, use_container_width=True)
        
        with col2:
            fig_time_dist = px.box(proc_data, y='total_days',
                                 title=f"Time Distribution - {selected_procedure}",
                                 labels={'total_days': 'Total Days'})
            st.plotly_chart(fig_time_dist, use_container_width=True)
        
        # Authority analysis
        st.subheader("🏛️ Regulatory Authority Analysis")
        authority_stats = proc_data.groupby('authority').agg({
            'official_fees': ['mean', 'std', 'count'],
            'total_days': ['mean', 'std']
        }).round(0)
        
        if not authority_stats.empty:
            authority_stats.columns = ['Avg_Cost', 'Std_Cost', 'Count', 'Avg_Days', 'Std_Days']
            st.dataframe(authority_stats, use_container_width=True)

def generate_powerbi_dataset(df, procedures_df):
    """Generate optimized dataset for Power BI"""
    
    # Create business-level dataset
    business_data = df[['interview_id', 'business_name', 'district', 'primary_sector', 
                       'business_size', 'legal_status', 'total_compliance_cost', 
                       'total_compliance_time', 'risk_score', 'employees_fulltime',
                       'employees_parttime', 'year_established']].copy()
    
    # Create procedures dataset
    if not procedures_df.empty:
        procedures_data = procedures_df[['interview_id', 'procedure', 'authority', 
                                       'official_fees', 'unofficial_payments', 'total_days',
                                       'complexity', 'application_mode', 'status']].copy()
    else:
        procedures_data = pd.DataFrame()
    
    # Merge for denormalized Power BI model
    if not procedures_data.empty and not business_data.empty:
        powerbi_data = pd.merge(business_data, procedures_data, on='interview_id', how='left')
    else:
        powerbi_data = business_data
    
    return powerbi_data


def display_data_export(analytics, df, procedures_df):
    """Display data export options"""
    st.header("🔗 Data Export & Integration")
    
    # Power BI Integration
    st.subheader("📊 Power BI Integration")
    
    st.info("""
    Use the data exports below to connect to Power BI for advanced analytics and reporting.
    Recommended connection method: Import CSV files directly into Power BI.
    """)
    
    # Generate comprehensive dataset for Power BI
    powerbi_data = generate_powerbi_dataset(df, procedures_df)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Main dataset export
        csv_main = df.to_csv(index=False)
        st.download_button(
            label="💾 Download Main Dataset (CSV)",
            data=csv_main,
            file_name=f"compliance_main_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    with col2:
        # Procedures dataset export
        if not procedures_df.empty:
            csv_procedures = procedures_df.to_csv(index=False)
            st.download_button(
                label="📋 Download Procedures Data (CSV)",
                data=csv_procedures,
                file_name=f"compliance_procedures_data_{datetime.now().strftime('%Y%m%d')}.csv",
                mime="text/csv",
                use_container_width=True
            )
    
    with col3:
        # Power BI optimized dataset
        csv_powerbi = powerbi_data.to_csv(index=False)
        st.download_button(
            label="🚀 Download Power BI Dataset (CSV)",
            data=csv_powerbi,
            file_name=f"powerbi_compliance_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
            use_container_width=True
        )
    
    # Image exports - WITH ERROR HANDLING
    st.subheader("🖼️ Chart Exports")
    
    if not df.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            try:
                # Cost distribution chart
                fig_cost = px.histogram(df, x='total_compliance_cost',
                                      title="Compliance Cost Distribution")
                img_cost = fig_cost.to_image(format="png")
                st.download_button(
                    label="📥 Download Cost Distribution",
                    data=img_cost,
                    file_name="cost_distribution.png",
                    mime="image/png",
                    use_container_width=True
                )
            except Exception as e:
                st.warning("📊 Chart export requires Kaleido. Install with: `pip install -U kaleido`")
                st.info("Chart preview:")
                st.plotly_chart(fig_cost, use_container_width=True)
        
        with col2:
            try:
                # Time distribution chart
                fig_time = px.histogram(df, x='total_compliance_time',
                                      title="Compliance Time Distribution")
                img_time = fig_time.to_image(format="png")
                st.download_button(
                    label="📥 Download Time Distribution",
                    data=img_time,
                    file_name="time_distribution.png",
                    mime="image/png",
                    use_container_width=True
                )
            except Exception as e:
                st.warning("📊 Chart export requires Kaleido. Install with: `pip install -U kaleido`")
                st.info("Chart preview:")
                st.plotly_chart(fig_time, use_container_width=True)          

def create_custom_query_tool():
    """Custom SQL query tool for advanced analysis"""
    st.header("🔧 Custom Query Tool")
    
    st.warning("""
    ⚠️ Advanced Feature: This tool allows direct SQL queries. 
    Use with caution and only if you're familiar with SQL.
    """)
    
    query = st.text_area("Enter your SQL query:", height=150,
                        value="SELECT * FROM responses LIMIT 10")
    
    if st.button("Execute Query", use_container_width=True):
        try:
            conn = sqlite3.connect('compliance_survey.db', check_same_thread=False)
            result_df = pd.read_sql(query, conn)
            conn.close()
            
            st.subheader("Query Results")
            st.dataframe(result_df, use_container_width=True)
            
            # Export results
            csv = result_df.to_csv(index=False)
            st.download_button(
                label="📥 Download Query Results",
                data=csv,
                file_name=f"query_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )
            
        except Exception as e:
            st.error(f"Query error: {str(e)}")

def create_interactive_dashboard():
    """Main interactive dashboard function"""
    st.title("📊 Compliance Analytics Dashboard")
    
    # Initialize analytics
    analytics = ComplianceAnalytics()
    df, procedures_df = analytics.get_analytics_data()
    
    if df.empty:
        st.warning("No submitted interviews available for analysis.")
        return
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Sector filter
    sectors = ['All'] + list(df['primary_sector'].unique())
    selected_sector = st.sidebar.selectbox("Sector", sectors)
    
    # District filter
    districts = ['All'] + list(df['district'].unique())
    selected_district = st.sidebar.selectbox("District", districts)
    
    # Business size filter
    sizes = ['All'] + list(df['business_size'].unique())
    selected_size = st.sidebar.selectbox("Business Size", sizes)
    
    # Apply filters
    filtered_df = df.copy()
    filtered_procedures = procedures_df.copy()
    
    if selected_sector != 'All':
        filtered_df = filtered_df[filtered_df['primary_sector'] == selected_sector]
        if not filtered_procedures.empty:
            filtered_procedures = filtered_procedures[filtered_procedures['primary_sector'] == selected_sector]
    
    if selected_district != 'All':
        filtered_df = filtered_df[filtered_df['district'] == selected_district]
        if not filtered_procedures.empty:
            filtered_procedures = filtered_procedures[filtered_procedures['district'] == selected_district]
    
    if selected_size != 'All':
        filtered_df = filtered_df[filtered_df['business_size'] == selected_size]
        if not filtered_procedures.empty:
            filtered_procedures = filtered_procedures[filtered_procedures['business_size'] == selected_size]
    
    # Dashboard tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📈 Overview", "💰 Cost Matrix", "🏢 Sector Analysis", "⏱️ Time Analysis", 
        "📋 Procedure Details", "🔗 Data Export"
    ])
    
    with tab1:
        display_overview_metrics(filtered_df, filtered_procedures)
    
    with tab2:
        display_cost_matrix(analytics, filtered_procedures)
    
    with tab3:
        display_sector_analysis(analytics, filtered_df, filtered_procedures)
    
    with tab4:
        display_time_analysis(filtered_procedures)
    
    with tab5:
        display_procedure_details(filtered_procedures)
    
    with tab6:
        display_data_export(analytics, filtered_df, filtered_procedures)

# Main analytics function for integration
def analytics_main():
    """Main analytics application - integrated version"""
    # Navigation within analytics
    st.sidebar.header("📊 Analytics Navigation")
    
    analytics_option = st.sidebar.radio(
        "Select Analytics Module:",
        ["Interactive Dashboard", "Custom Query Tool", "About Analytics"]
    )
    
    if analytics_option == "Interactive Dashboard":
        create_interactive_dashboard()
    elif analytics_option == "Custom Query Tool":
        create_custom_query_tool()
    else:
        st.title("About Compliance Analytics")
        st.info("""
        **Compliance Analytics Dashboard**
        
        This module provides comprehensive analytics for regulatory compliance data,
        including:
        
        - 📈 **Overview Metrics**: Key performance indicators and distributions
        - 💰 **Cost Matrix**: Detailed cost analysis across procedures
        - 🏢 **Sector Analysis**: Comparative analysis by business sector
        - ⏱️ **Time Analysis**: Processing time and efficiency metrics
        - 📋 **Procedure Details**: Deep dive into specific procedures
        - 🔗 **Data Export**: Power BI integration and data exports
        
        **Data Sources**: compliance_survey.db database
        **Export Formats**: CSV, PNG images, Power BI optimized datasets
        """)