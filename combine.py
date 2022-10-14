import json


# builds the children records for a patients multiple records (e.g., treatments 0->N)
def build_children(patient_id, parent, children_records, element_name):
    children = []
    for child in children_records:
        child_id = child["case_accession_number"]
        if child_id == patient_id:
            #print(s)
            children.append(child)
            
    if len(children) > 0:
        parent.update({element_name: children})


# START HERE

# single combined JSON for all patients
outfile = open("combined.json", "w") 
outfile.write("[\r")

# Load all the files you wish to combine with the patient master
with open('patient_hx.json', 'r') as history:
    all_history = json.load(history)

with open('patient_tissue.json', 'r') as accessions:
    all_accessions = json.load(accessions)

with open('patient_staging.json', 'r') as stagings:
    all_stages = json.load(stagings)

with open('patient_procedure.json', 'r') as procedures:
    all_procedures = json.load(procedures)

with open('patient_treatment.json', 'r') as treatments:
    all_treatments= json.load(treatments)

with open('patient_bmarker.json', 'r') as bmarker:
    all_bmarkers = json.load(bmarker)

with open('patient_molecular.json', 'r') as mole:
    all_molecular = json.load(mole)

with open('patient_frailty.json', 'r') as frailty:
    all_frailty = json.load(frailty)

with open('patient_status.json', 'r') as outcomes:
    all_outcomes = json.load(outcomes)

with open('patient.json', 'r') as patients:
    all_patients = json.load(patients)

for patient in all_patients:
    print(patient)
    pat_id = str(patient["case_accession_number"])

# start adding children records to patient master
    build_children(pat_id, patient, all_history, "history")

    build_children(pat_id, patient, all_stages, "staging")

    build_children(pat_id, patient, all_procedures, "procedures")

    build_children(pat_id, patient, all_treatments, "treatments")

    build_children(pat_id, patient, all_accessions, "accessions")

    build_children(pat_id, patient, all_stages, "staging")

    build_children(pat_id, patient, all_bmarkers, "bio_markers")

    build_children(pat_id, patient, all_molecular, "molecular")

    build_children(pat_id, patient, all_frailty, "frailty")

    build_children(pat_id, patient, all_outcomes, "outcomes")


    # dump the patient to a file
    outfile.write(json.dumps(patient, separators=(',', ': '), indent=4))
    outfile.write(",\r")

outfile.write("]\r")
