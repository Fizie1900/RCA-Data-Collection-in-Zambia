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

try:
    from draft_manager import DraftManager, display_draft_dashboard, display_draft_quick_access, load_draft_into_session
except ImportError:
    class DraftManager:
        def __init__(self):
            pass
        def get_user_drafts(self, username):
            return pd.DataFrame()
        def get_all_drafts(self):
            return pd.DataFrame()
        def load_draft(self, interview_id):
            return None
        def update_draft_progress(self, interview_id, current_section, progress_percentage):
            return False
        def delete_draft(self, interview_id):
            return False
        def calculate_progress(self, form_data, current_section):
            return 0
    
    def display_draft_dashboard():
        st.info("📝 Draft Manager not available")
    
    def display_draft_quick_access():
        pass
    
    def load_draft_into_session(draft_manager, interview_id):
        st.error("Draft manager not available")

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
            '4100': 'Construction of buildings',
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
            '4759': 'Retail sale of other household appliances in specialized stores',
            '4761': 'Retail sale of books, newspapers and stationery in specialized stores',
            '4762': 'Retail sale of music and video recordings in specialized stores',
            '4763': 'Retail sale of sporting equipment in specialized stores',
            '4764': 'Retail sale of games and toys in specialized stores',
            '4771': 'Retail sale of clothing, footwear and leather articles in specialized stores',
            '4772': 'Retail sale of pharmaceutical and medical goods, cosmetic and toilet articles in specialized stores',
            '4773': 'Other retail sale of new goods in specialized stores',
            '4774': 'Retail sale of second-hand goods in specialized stores',
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
            '5621': 'Event catering activities',
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
            '6499': 'Other financial service activities, except insurance and pension funding',
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
            '8710': 'Residential nursing care activities',
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

# Enhanced ISIC data management with pyisic integration
@st.cache_data(ttl=3600)  # Cache for 1 hour
def search_isic_codes_enhanced(search_term: str, isic_client) -> List[Dict[str, Any]]:
    """
    Enhanced ISIC search using pyisic library
    
    Args:
        search_term: Term to search for
        isic_client: pyisic client instance
        
    Returns:
        List of formatted ISIC code results
    """
    if not search_term:
        return []
    
    search_term = str(search_term).strip()
    
    # Use pyisic for search
    with st.spinner("🔍 Searching ISIC classification..."):
        results = isic_client.search_isic_codes(search_term)
    
    if results:
        return results
    
    return []

def auto_populate_business_activities(isic_result: Dict[str, Any]):
    """
    Auto-populate business activities when an ISIC code is selected
    
    Args:
        isic_result: Selected ISIC code information
    """
    current_activities = st.session_state.business_activities_text.strip()
    new_activity = f"{isic_result['title']} (ISIC: {isic_result['code']})"
    
    if not current_activities:
        # If no activities yet, set as the main activity
        st.session_state.business_activities_text = new_activity
    elif new_activity not in current_activities:
        # If activities exist but this one isn't included, append it
        separator = "; " if not current_activities.endswith(';') else " "
        st.session_state.business_activities_text = current_activities + separator + new_activity

def display_selected_isic_code(code, index, prefix):
    """Display a selected ISIC code with remove option"""
    col1, col2 = st.columns([4, 1])
    
    with col1:
        st.write(f"• {code}")
    
    with col2:
        if st.button("🗑️", key=f"remove_{prefix}_{index}"):
            st.session_state.selected_isic_codes.pop(index)
            st.rerun()

# Enhanced Business Activities with pyisic Integration
def business_activities_section():
    """Enhanced business activities section with pyisic integration"""
    
    st.subheader("🏢 Business Activities & ISIC Classification")
    
    # Initialize pyisic client
    if st.session_state.isic_client is None:
        st.session_state.isic_client = PyISICClient()
    
    isic_client = st.session_state.isic_client
    
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
    
    # ISIC Code Selection Section with pyisic
    st.subheader("📊 ISIC Code Classification")
    
    # Real-time search with auto-suggest
    search_col1, search_col2 = st.columns([3, 1])
    
    with search_col1:
        search_term = st.text_input(
            "🔍 Search ISIC codes (type to search automatically):",
            placeholder="e.g., agriculture, construction, 0111, manufacturing",
            key="isic_search_main",
            value=st.session_state.isic_search_term
        )
    
    with search_col2:
        st.write("")  # Spacer
        st.write("")  # Spacer
        if st.button("🔄 Clear Search", use_container_width=True, key="clear_search"):
            st.session_state.isic_search_term = ""
            st.rerun()
    
    # Update search term in session state and perform search
    if search_term != st.session_state.isic_search_term:
        st.session_state.isic_search_term = search_term
    
    # Perform search when there's a search term (auto-search)
    if st.session_state.isic_search_term and len(st.session_state.isic_search_term) >= 2:
        search_results = search_isic_codes_enhanced(
            st.session_state.isic_search_term, 
            isic_client
        )
        
        if search_results:
            st.write(f"📋 **Found {len(search_results)} matching ISIC codes:**")
            
            # Display results in a compact format with auto-select
            for i, result in enumerate(search_results):
                col1, col2, col3, col4 = st.columns([3, 1, 1, 1])
                
                with col1:
                    # Highlight matching terms
                    display_text = f"**{result['code']}** - {result['title']}"
                    st.write(display_text)
                    if result.get('description') and result['description'] != result['title']:
                        st.caption(f"{result['description']}")
                
                with col2:
                    if st.button("➕ Select", key=f"select_{i}"):
                        if result['display'] not in st.session_state.selected_isic_codes:
                            st.session_state.selected_isic_codes.append(result['display'])
                            # Auto-populate business activities
                            auto_populate_business_activities(result)
                            st.success(f"✅ Added: {result['code']} - {result['title']}")
                            st.rerun()
                
                with col3:
                    if st.button("ℹ️ Details", key=f"details_{i}"):
                        # Show detailed information
                        with st.expander(f"📋 Details for ISIC {result['code']}", expanded=True):
                            st.write(f"**Code:** {result['code']}")
                            st.write(f"**Title:** {result['title']}")
                            if result.get('description'):
                                st.write(f"**Description:** {result['description']}")
                            if result.get('version'):
                                st.write(f"**Version:** {result['version']}")
                            if result.get('level'):
                                st.write(f"**Level:** {result['level']}")
                
                with col4:
                    if st.button("📋 Copy", key=f"copy_{i}"):
                        # Copy to clipboard (simulated)
                        st.success(f"📋 Copied: {result['code']} - {result['title']}")
            
            st.markdown("---")
        elif st.session_state.isic_search_term:
            st.warning("No ISIC codes found. Try different search terms.")
    
    # Popular ISIC categories quick access
    st.write("**🚀 Quick Access - Popular Categories:**")
    quick_col1, quick_col2, quick_col3, quick_col4, quick_col5 = st.columns(5)
    
    with quick_col1:
        if st.button("🌾 Agriculture", use_container_width=True, key="quick_agri"):
            st.session_state.isic_search_term = "agriculture"
            st.rerun()
    
    with quick_col2:
        if st.button("🏗️ Construction", use_container_width=True, key="quick_constr"):
            st.session_state.isic_search_term = "construction"
            st.rerun()
    
    with quick_col3:
        if st.button("🛒 Retail", use_container_width=True, key="quick_retail"):
            st.session_state.isic_search_term = "retail"
            st.rerun()
    
    with quick_col4:
        if st.button("💻 IT Services", use_container_width=True, key="quick_it"):
            st.session_state.isic_search_term = "computer"
            st.rerun()
    
    with quick_col5:
        if st.button("🏥 Healthcare", use_container_width=True, key="quick_health"):
            st.session_state.isic_search_term = "health"
            st.rerun()
    
    # Manual ISIC code entry with validation
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
                    # Validate code format
                    if isic_client.validate_isic_code(custom_code):
                        custom_display = f"{custom_code} - {custom_description}"
                        if custom_display not in st.session_state.selected_isic_codes:
                            st.session_state.selected_isic_codes.append(custom_display)
                            auto_populate_business_activities({
                                'code': custom_code,
                                'title': custom_description,
                                'description': custom_description
                            })
                            st.success(f"✅ Added custom ISIC code: {custom_code}")
                            st.rerun()
                    else:
                        st.warning(f"⚠️ '{custom_code}' doesn't appear to be a valid ISIC code format")
                else:
                    st.error("❌ Please enter both code and description")
    
    # Display selected ISIC codes
    if st.session_state.selected_isic_codes:
        st.subheader("✅ Selected ISIC Codes")
        
        # Group codes by category for better organization
        agri_codes = [code for code in st.session_state.selected_isic_codes if any(x in code for x in ['01', '02', '03'])]
        construction_codes = [code for code in st.session_state.selected_isic_codes if any(x in code for x in ['41', '42', '43'])]
        retail_codes = [code for code in st.session_state.selected_isic_codes if any(x in code for x in ['45', '46', '47'])]
        service_codes = [code for code in st.session_state.selected_isic_codes if any(x in code for x in ['55', '56', '62', '63', '64', '65', '66'])]
        manufacturing_codes = [code for code in st.session_state.selected_isic_codes if any(x in code for x in ['10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32'])]
        other_codes = [code for code in st.session_state.selected_isic_codes if code not in agri_codes + construction_codes + retail_codes + service_codes + manufacturing_codes]
        
        if agri_codes:
            with st.expander("🌾 Agriculture, Forestry & Fishing", expanded=True):
                for i, code in enumerate(agri_codes):
                    display_selected_isic_code(code, i, "agri")
        
        if manufacturing_codes:
            with st.expander("🏭 Manufacturing", expanded=True):
                for i, code in enumerate(manufacturing_codes):
                    display_selected_isic_code(code, i, "manuf")
        
        if construction_codes:
            with st.expander("🏗️ Construction", expanded=True):
                for i, code in enumerate(construction_codes):
                    display_selected_isic_code(code, i, "constr")
        
        if retail_codes:
            with st.expander("🛒 Retail & Wholesale", expanded=True):
                for i, code in enumerate(retail_codes):
                    display_selected_isic_code(code, i, "retail")
        
        if service_codes:
            with st.expander("💼 Services", expanded=True):
                for i, code in enumerate(service_codes):
                    display_selected_isic_code(code, i, "service")
        
        if other_codes:
            with st.expander("📦 Other Sectors", expanded=True):
                for i, code in enumerate(other_codes):
                    display_selected_isic_code(code, i, "other")
        
        # Summary
        total_codes = len(st.session_state.selected_isic_codes)
        st.info(f"**Total ISIC codes selected:** {total_codes}")
        
        # Clear all button
        if st.button("🗑️ Clear All Selected Codes", key="clear_all_isic"):
            st.session_state.selected_isic_codes = []
            st.rerun()
    
    return business_activities

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
        'app_mode': 'login',
        'database_initialized': False,
        'isic_client': None
    }
    
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value

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

# Section A - Business Profile with pyisic Integration
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
    
    # ISIC Code section with pyisic integration
    st.markdown("---")
    business_activities_section()

# [Include all other sections B, C, D and admin functions from previous complete versions]
# For brevity, I'm showing the key integration points. The other sections remain the same.

# Main Application
def main():
    # Initialize session state first
    initialize_session_state()
    
    # Test database connection first
    if not test_connection():
        st.error("Cannot proceed without database connection")
        st.info("Please check your SQLite Cloud connection string and try again.")
        return
    
    # Initialize database
    if not st.session_state.get('database_initialized', False):
        with st.spinner("🔄 Setting up database..."):
            if check_and_fix_database():
                st.session_state.database_initialized = True
            else:
                st.error("Failed to initialize database. Please check your connection.")
                return
    
    # Initialize pyisic client
    if st.session_state.isic_client is None:
        st.session_state.isic_client = PyISICClient()
    
    # Run database migrations
    add_missing_columns()
    
    # Route based on login status
    if not st.session_state.interviewer_logged_in and not st.session_state.admin_logged_in:
        login_system()
    elif st.session_state.admin_logged_in:
        admin_navigation()
    else:
        data_collection_navigation()

if __name__ == "__main__":
    main()
