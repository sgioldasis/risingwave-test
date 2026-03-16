# uv run --with fpdf2 python scripts/generate_use_cases_slides.py
from fpdf import FPDF
import os

class SlideDeck(FPDF):
    def __init__(self):
        # 16:9 aspect ratio standard presentation size (e.g., 254 x 142.875 mm)
        super().__init__(orientation='L', unit='mm', format=(142.875, 254))
        self.set_auto_page_break(auto=False)
        self.set_margins(15, 15, 15)

    def add_title_slide(self, title, subtitle):
        self.add_page()
        # Background
        self.set_fill_color(248, 249, 250)
        self.rect(0, 0, 254, 142.875, 'F')
        
        # Title
        self.set_font("helvetica", style="B", size=32)
        self.set_text_color(30, 58, 138)  # Dark Blue
        self.set_y(60)
        self.cell(0, 15, title, align='C')
        
        # Subtitle
        self.set_y(80)
        self.set_font("helvetica", size=18)
        self.set_text_color(107, 114, 128)  # Gray
        self.cell(0, 10, subtitle, align='C')

    def draw_slide_template(self, title):
        self.add_page()
        # Header background
        self.set_fill_color(30, 58, 138)  # Dark Blue
        self.rect(0, 0, 254, 25, 'F')
        
        # Title
        self.set_font("helvetica", style="B", size=24)
        self.set_text_color(255, 255, 255)
        self.set_y(7)
        self.cell(0, 10, title, align='L')
        
        # Reset colors for body
        self.set_text_color(55, 65, 81)
        
    def add_bullet(self, text, y_pos):
        self.set_y(y_pos)
        self.set_x(20)
        
        # Draw bullet point
        self.set_fill_color(59, 130, 246)  # Blue bullet
        self.rect(20, y_pos + 2, 3, 3, 'F')
        
        # Draw text
        self.set_x(28)
        
        # Handle bold prefix (format: "BoldText: NormalText")
        if ":" in text:
            bold_part, normal_part = text.split(":", 1)
            self.set_font("helvetica", style="B", size=14)
            width = self.get_string_width(bold_part + ": ")
            self.cell(width, 7, bold_part + ":", border=0)
            
            self.set_font("helvetica", size=14)
            self.cell(0, 7, normal_part, border=0)
        else:
            self.set_font("helvetica", size=14)
            self.cell(0, 7, text, border=0)

    def draw_flow_infographic(self, nodes, edges, start_y=85):
        # Draw bounding box for the infographic area
        self.set_fill_color(243, 244, 246)  # Very light gray
        self.rect(15, start_y, 224, 50, 'F')
        
        # Configure nodes
        node_width = 40
        node_height = 20
        spacing = (224 - (len(nodes) * node_width)) / (len(nodes) + 1)
        
        # Draw edges (arrows) first so they go under nodes
        self.set_draw_color(156, 163, 175)  # Gray line
        self.set_line_width(1.5)
        
        for i in range(len(nodes) - 1):
            x1 = 15 + spacing + (node_width + spacing) * i + node_width
            y1 = start_y + 25
            x2 = 15 + spacing + (node_width + spacing) * (i + 1)
            y2 = start_y + 25
            
            # Line
            self.line(x1, y1, x2 - 2, y2)
            
            # Arrow head
            self.line(x2 - 4, y2 - 2, x2, y2)
            self.line(x2 - 4, y2 + 2, x2, y2)
            
            # Edge label if any
            if i < len(edges):
                self.set_font("helvetica", style="I", size=9)
                self.set_text_color(107, 114, 128)
                label_w = self.get_string_width(edges[i])
                label_x = x1 + (x2 - x1 - label_w) / 2
                self.set_xy(label_x, y1 - 6)
                self.cell(label_w, 5, edges[i], align='C')

        # Draw nodes
        for i, node in enumerate(nodes):
            x = 15 + spacing + (node_width + spacing) * i
            y = start_y + 15
            
            # Node box shadow
            self.set_fill_color(209, 213, 219)
            self.rect(x+1, y+1, node_width, node_height, 'F', round_corners=True, corner_radius=2)
            
            # Node box
            if node['type'] == 'source':
                color = (236, 253, 245)  # Light Green
                border = (16, 185, 129)
            elif node['type'] == 'process':
                color = (239, 246, 255)  # Light Blue
                border = (59, 130, 246)
            elif node['type'] == 'storage':
                color = (255, 247, 237)  # Light Orange
                border = (245, 158, 11)
            else: # serve/sink
                color = (245, 243, 255)  # Light Purple
                border = (139, 92, 246)
                
            self.set_fill_color(*color)
            self.set_draw_color(*border)
            self.set_line_width(0.5)
            self.rect(x, y, node_width, node_height, 'DF', round_corners=True, corner_radius=2)
            
            # Node Role (Top smaller text)
            self.set_font("helvetica", style="B", size=8)
            self.set_text_color(*border)
            self.set_xy(x, y + 2)
            self.cell(node_width, 4, node['role'].upper(), align='C')
            
            # Node Main Label
            self.set_font("helvetica", style="B", size=10)
            self.set_text_color(31, 41, 55)
            self.set_xy(x, y + 7)
            self.cell(node_width, 5, node['label'], align='C')
            
            # Node Sub Label
            self.set_font("helvetica", size=8)
            self.set_text_color(107, 114, 128)
            self.set_xy(x, y + 12)
            self.cell(node_width, 5, node['sub'], align='C')

def generate_pdf():
    pdf = SlideDeck()
    
    # Title Slide
    pdf.add_title_slide(
        "Real-Time E-Commerce Conversion Funnel", 
        "Use Cases & Architecture Overview"
    )
    
    # Slide 1
    pdf.draw_slide_template("Use Case 1: Push Model - Modern Dashboard")
    pdf.add_bullet("Decoupled Design: Backend never queries RisingWave directly.", 40)
    pdf.add_bullet("Real-Time Stream: Data flows from RisingWave to Dashboard via Kafka.", 52)
    pdf.add_bullet("Scalability: Kafka buffers data, ensuring resilience during outages.", 64)
    
    nodes1 = [
        {"role": "Ingest", "label": "Kafka Sources", "sub": "Events", "type": "source"},
        {"role": "Process", "label": "RisingWave", "sub": "Materialized Views", "type": "process"},
        {"role": "Stream", "label": "Kafka Sink", "sub": "Funnel Topic", "type": "process"},
        {"role": "Serve", "label": "Dashboard", "sub": "React + SSE", "type": "serve"}
    ]
    edges1 = ["SQL JOIN", "JSON Push", "Consume"]
    pdf.draw_flow_infographic(nodes1, edges1)
    
    # Slide 2 (Updated for River Online Learning)
    pdf.draw_slide_template("Use Case 2: ML Training & Serving (River Online)")
    pdf.add_bullet("Online Learning: River models update instantly via streaming data.", 40)
    pdf.add_bullet("Instant Adaptation: Responds immediately to concept drift.", 52)
    pdf.add_bullet("Low Latency: Millisecond predictions from memory (no batch lags).", 64)
    
    nodes2 = [
        {"role": "Aggregated Data", "label": "RisingWave/Kafka", "sub": "Streaming Features", "type": "source"},
        {"role": "Learn", "label": "River Learner", "sub": "learn_one()", "type": "process"},
        {"role": "Checkpoint", "label": "MinIO", "sub": "Model State", "type": "storage"},
        {"role": "Predict", "label": "FastAPI", "sub": "river_predictor", "type": "serve"}
    ]
    edges2 = ["Fetch", "Save State", "Load/Query"]
    pdf.draw_flow_infographic(nodes2, edges2)
    
    # Slide 3
    pdf.draw_slide_template("Use Case 3: Historical Iceberg Integration")
    pdf.add_bullet("Instant Sync: Iceberg updates are immediately visible in RisingWave.", 40)
    pdf.add_bullet("Enriched Analytics: Joining event streams with deep historical data.", 52)
    pdf.add_bullet("Native Connector: High-performance access via Lakekeeper REST.", 64)
    
    nodes3 = [
        {"role": "Update", "label": "Trino SQL", "sub": "External Writes", "type": "source"},
        {"role": "Storage", "label": "Apache Iceberg", "sub": "Lakekeeper REST", "type": "storage"},
        {"role": "Sync", "label": "RisingWave Source", "sub": "Iceberg Connector", "type": "process"},
        {"role": "Enrich", "label": "Materialized View", "sub": "events + countries", "type": "serve"}
    ]
    edges3 = ["Write", "Auto-Refresh", "JOIN"]
    pdf.draw_flow_infographic(nodes3, edges3)
    
    output_path = "Use_Cases_Slides_Updated.pdf"
    pdf.output(output_path)
    print(f"✅ Generated {output_path} successfully!")
    
if __name__ == "__main__":
    generate_pdf()
