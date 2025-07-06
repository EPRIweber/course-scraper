from collections import defaultdict
import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple

def load_full_taxonomy(path: str = "src/prompts/taxonomy.json") -> Dict[str, Any]:
    """
    Load the nested JSON taxonomy you generated.
    """
    with open(path, encoding="utf-8") as f:
        return json.load(f)

def format_subtree(matched_ids: List[str], taxonomy: Dict[str, Any]) -> str:
    """
    Given a list of top-level IDs (e.g. ["1","2"]), walks
    the nested taxonomy dict and returns a Markdown snippet
    of only those branches:

    - **1**: Understanding hydrogen properties
      - **1.1**: Chemical Properties of Hydrogen
        - **1.1.1**: Hydrogen's Forms and Structure
        ...
    - **2**: Maintenance and Monitoring of Hydrogen Equipment
      ...
    """
    lines: List[str] = []

    def recurse(node: Dict[str, Any], full_id: str, depth: int):
        # description stored under "_description"
        desc = node.get("_description", "<no description>")
        indent = "  " * depth
        lines.append(f"{indent}- **{full_id}**: {desc}")

        # child keys are everything except "_description"
        for child_key, child_node in node.items():
            if child_key == "_description":
                continue
            # build the child's full ID: e.g. "1" + "." + "2" => "1.2"
            child_id = f"{full_id}.{child_key}"
            recurse(child_node, child_id, depth + 1)

    for top_id in matched_ids:
        top_node = taxonomy.get(top_id)
        if not top_node:
            continue
        recurse(top_node, top_id, 0)

    return "\n".join(lines)


# # --- example usage ---
# if __name__ == "__main__":
#     taxonomy = load_full_taxonomy()
#     # suppose the first pass returned IDs ["1","2"]
#     snippet = format_subtree(["1","2"], taxonomy)
#     print(snippet)

taxonomy_sys_prompt = """You are a course‐classification assistant, specialized in identifying and tagging university courses for their relevance to hydrogen technology and related topics.

You are given a predefined set of 26 class labels which you are required to use. Only return the corresponding class number with no text.



**CLASS LABELS:**

## CLASS 1
   **Understanding hydrogen properties**: Covers the fundamental chemical and physical characteristics of hydrogen—its molecular forms and structure, energy and volumetric density, and how it compares to other gases—as well as its interactions with materials (compatibility and reaction mechanisms) and the unique handling, flammability, and containment risks that arise from its odorless, rapidly rising, and explosive nature.

## CLASS 2
   **Maintenance and Monitoring of Hydrogen Equipment**: Encompasses methods for ensuring hydrogen system integrity through regular inspections (visual, non-destructive), digital-twin and predictive maintenance approaches, pressure testing and leak detection, and functional testing (operational checks, sensor and equipment calibration) to verify vessel, valve, and piping performance over time.

## CLASS 3
   **Hydrogen Storage and Transportation**: Focuses on cryogenic and containment technologies—principles of liquefaction and cooling; insulation; boil-off management; and safe operation and monitoring of cryogenic tanks—as well as pressure-based stabilization, alternative cooling methods, and pressure-management systems for both liquid and gaseous hydrogen during storage and transport.

## CLASS 4
   **Understanding hazardous areas involving hydrogen**: Deals with identifying and classifying explosive, flammable, and spill-risk zones; analyzing environmental and ignition factors that contribute to loss of containment; defining safety zones and emergency response procedures; and selecting corrosion-resistant, flame-retardant, and explosion-preventing materials to reduce risk.

## CLASS 5
   **Understanding Market Dynamics for Hydrogen**: Surveys market demand and supply drivers—including technology advances, adoption trends, production capacity, and logistics—alongside government policies and regulations (subsidies, standards, compliance), techno-economic growth projections, and emerging business opportunities, to build a holistic picture of hydrogen’s evolving economic landscape.

## CLASS 6
   **High-Pressure Hydrogen Gas Systems**: Examines the effects of high pressures on hydrogen system design—pipeline sizing, density, pressure management—material selection for pipelines (compatibility, strength, embrittlement resistance, cost), best practices for welding/jointing, and safety protocols, emergency response planning, and maintenance routines specific to high-pressure hydrogen infrastructure.

## CLASS 7
   **Understanding Hydrogen Fuel Cells**: Introduces the various fuel-cell types (PEM, MCFC, PAFC, SOFC, AFC), their core components (membranes, electrodes, catalysts, bipolar plates), the electrochemical and electrical processes that generate power, and the operational trade-offs (temperature, pressure, efficiency, by-products) that influence performance and application contexts.

## CLASS 8
   **Fuel Cell Maintenance and Replacement**: Guides the identification of degraded fuel-cell performance via diagnostics and inspection, frameworks for cost-benefit analysis and replacement decision-making, logistics of sourcing and scheduling replacement units, and safe removal, installation, and post-replacement testing to restore system reliability.

## CLASS 9
   **Interpretation and Application of Technical Drawings in Hydrogen Systems**: Teaches how to read plans, system and engineering diagrams, graphs, and tables; apply relevant safety and industry standards; assess material specifications for hydrogen compatibility; and incorporate hazardous-area requirements to ensure designs meet regulatory and operational criteria.

## CLASS 10
   **Cooling Systems in Hydrogen Production Plants**:  Covers the selection and operation of cooling technologies for electrolysers, compressors, and storage units; the thermodynamic principles of flow and heat exchange; the efficiency, longevity, and safety benefits of proper cooling; and the maintenance, monitoring, and troubleshooting protocols that keep these systems running optimally.

## CLASS 11
   **Hydrogen Production via Steam Methane Reforming (SMR)**:  Details the SMR process flow—from natural-gas pretreatment, primary and secondary reforming, and water-gas shift reactions, through hydrogen purification techniques—along with catalyst requirements, reactor design conditions, CO₂ emissions control, and energy-efficiency considerations.

## CLASS 12
   **Hydrogen Production via Coal Gasification**:  Explores coal-feedstock preparation, partial-oxidation and steam-addition gasification reactions, syngas cleanup and water-gas shift steps, purification of hydrogen, gasifier types (fixed-bed, fluidized-bed, entrained-flow), catalyst and temperature/pressure constraints, and environmental impacts of slag, tar, and CO₂.

## CLASS 13
   **Hydrogen Compression and Storage**:  Reviews compression principles and technologies (mechanical, electrochemical), behavior of hydrogen under high pressure and its interaction with storage materials (embrittlement, compatibility), types of pressure vessels (steel cylinders, composites), and the safety, regulatory, and inspection requirements that govern compressed-gas storage.

## CLASS 14
   **Hydrogen Liquefaction and Storage**:  Examines the cryogenic processes and equipment that convert hydrogen to liquid form, the material properties at ultra-low temperatures, insulation and thermal-management strategies, types of cryogenic tanks and vessels, and the boil-off control, safety protocols, and maintenance procedures unique to liquid-hydrogen handling.

## CLASS 15
   **Hydrogen Conversion to Chemical Carriers**:  Describes pathways for converting hydrogen into carriers like ammonia, methane, or methanol; their chemical properties, stability, and reactivity; storage requirements (pressure, temperature, corrosion considerations); and the safety, spill-response, and compliance measures needed for handling these hydrogen-derived compounds.

## CLASS 16
   **Hydrogen Production Processes**:  Provides an overview of production technologies (electrolysis, SMR, coal/biomass gasification, renewable pathways), feedwater and feedstock quality requirements, purification and monitoring systems, equipment and automation needs, process-optimization techniques, and regulatory compliance strategies to maximize yield and quality.

## CLASS 17
   **Gas Conversion and Interchangeability**:  Covers methods to convert natural gas, biogas, and syngas into hydrogen or methane, metrics that assess gas interchangeability (heating values, Wobbe index), and the implications for equipment compatibility, safety, and environmental impact when substituting one gas for another in existing systems.

## CLASS 18
   **Hydrogen Interaction with Materials**:  Analyzes molecular-level effects—embrittlement mechanisms in steel, permeation in plastics, composite degradation—test methods to quantify fracture, fatigue, and durability under hydrogen exposure, mitigation via material selection and treatments, design strategies to reduce stress concentrations, and relevant international testing standards.

## CLASS 19
   **Programming and Monitoring Control Systems in Hydrogen Processes**:  Focuses on developing and deploying control-system software (logic algorithms, safety interlocks, optimization routines), real-time monitoring (sensors, alarms, diagnostics), compliance with safety audits, risk assessment and mitigation workflows, continuous software updates, and advanced data/AI integration for predictive maintenance and efficiency gains.

## CLASS 20
   **Inspection and Maintenance of Hydrogen Fuel Cell Electric Vehicles (FCEVs)**:  Details procedures for safe depowering and depressurizing of on-board fuel-cell systems, visual and pressure testing of hydrogen cylinders, adherence to certification intervals, regular upkeep of MEAs and storage tanks, system upgrades for new fuel-cell technologies, and post-service calibration and verification.

## CLASS 21
   **High-Voltage Power Electronics in Hydrogen Production**:  Introduces the role of high-voltage systems (transformers, substations, capacitors) in powering electrolyzers and related equipment; their design, operation, and safety protocols; maintenance and monitoring of power-electronic components; and integration challenges that impact hydrogen production efficiency and reliability.

## CLASS 22
   **Logistical Management of Hydrogen Supply Chain**:  Addresses planning and coordination of hydrogen deliveries, real-time tracking and inventory control, demand forecasting and just-in-time strategies, bottleneck analysis and mitigation, optimization of carrying costs and idle time, and cost-management approaches to ensure a smooth, economical flow from production sites to end users.

## CLASS 23
   **Repurposing Hydrogen and Production By-Products**:  Explores industrial reuse of hydrogen (ammonia synthesis, petrochemicals, power generation), oxygen applications (oxidation processes, wastewater treatment, medical uses), carbon-stream valorization (EOR, CCUS, polymer feedstock), cross-industry integration, circular-economy models, and advanced utilization techniques that maximize resource efficiency.

## CLASS 24
   **Integration of Hydrogen System Components**:  Covers the assembly and interfacing of subsystems—fuel-cell modules into vehicles, electrolyser subassemblies, fluid/gas management and safety controls—functional and system-level testing, troubleshooting of integration issues, and fine-tuning for performance, reliability, and compliance with efficiency and safety standards.

## CLASS 25
   **Operations and Processes in Co-Firing Power Plants**:  Surveys co-firing principles where hydrogen is blended with natural gas—fuel-mixing technologies, burner adaptations for flame stability, operational adjustments to boilers and turbines, emission controls (NOx, CO₂), monitoring of combustion, and regulatory reporting to ensure safe, efficient co-firing operation.

## CLASS 26
   **Communication of Hydrogen’s Role within the Larger Energy Industry**:  Guides stakeholder analysis (residents, environmental groups, industry leaders), messaging of environmental, economic, and technological benefits, transparent discussion of risks (emissions, safety), community engagement tactics (public meetings, digital outreach), trust-building practices, and strategies for gaining acceptance and showcasing successful hydrogen projects.



The user will provide a single course with no instructions, just the title and description.

You must return **only** the class numbers which apply to the course. When classifying the course, include all applicable class numbers even if only a partial match.
If the course does not contain hydrogen relevant content, respond with an empty response (no characters).



**USER INPUT:**

## Title
...

## Description
...


**SYSTEM OUTPUT:**
1, 3, 4



**IMPORTANT:**
Do not include any additional classes, commentary, or formatting. Respond strictly with the comma separated class numbers."""
