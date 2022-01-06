import json
import argparse
import os
import re


SEMANTIC_MAP = []
OUTPATH = ""
INPATH = ""

def open_semantic_map():
    try:
        with open("semantic_types.json", 'r') as f:
            return json.load(f)
    except Exception as e:
        raise e

def open_concept_map(fname):
    try:
        with open(fname, 'r') as f:
            return json.load(f)
    except Exception as e:
        raise e

def remove_dups(concept_map):
    a_set = {}
    a_set = set()
    # pass 1 get unique list
    for c in concept_map:
        concept_text = c["matchedtext"]
        a_set.add(concept_text)

    short_list = []
    for x in a_set:
        for c in concept_map:
            if x == c["matchedtext"]:
                short_list.append(c)
                break
    return short_list

def open_report(fname):
    try:
        with open(fname, 'r') as f:
            return f.readlines()

    except Exception as e:
        raise e

def get_semantic_color(semantic_type):
    try:
        for c in SEMANTIC_MAP:
            if semantic_type == c["code"]:
                #print(c)
                color = c["color"]
                if color == "":
                    color = "DimGray"
                return color
    except Exception as e:
        return "blue"

def get_semantic_meta(semantic_type):
    try:
        for c in SEMANTIC_MAP:
            if semantic_type == c["code"]:
                #print('Found {} code {}'.format(semantic_type, c))
                return c
            #print(c)
    except Exception as e:
        print(e)
        return None

def generate_annotation(concept_file, report_file, out_file):

    temp_map = open_concept_map(concept_file)
    concept_map = remove_dups(temp_map)
    report_text = open_report(report_file)

    stats = 'STATS:  report: {} report lines: {}  concept total: {} '.format(report_file, len(report_text), len(concept_map))

    annotated_report = ""
    
    processed_already = []

    idx = 0
    for line in report_text:
        sub_line = ""
        for c in concept_map:
            concept_text = c["matchedtext"]

            # form a regex parttern from the concept
            pattern = r"\b" + concept_text + r"\b"

            semantic_type = ""
            cui = ""
            pref_name = ""
            semantic_type_info = "Information Unavaliable"
            try:
                semantic_type = c["evlist"][0]["conceptinfo"]["semantictypes"][0]   # get get one for now
                cui = c["evlist"][0]["conceptinfo"]["cui"]
                pref_name = c["evlist"][0]["conceptinfo"]["preferredname"]
                    #color = get_semantic_color(semantic_type)
                semantic_type_meta = get_semantic_meta(semantic_type)
                color = (semantic_type_meta["color"], 'black')[semantic_type_meta["color"] == '']

                semantic_type_info = '{} | {} | ({})'.format(pref_name, semantic_type_meta["name"], cui)
            except Exception as e:
                print(e)
            
            sub_string = '<div class="tooltip" style="color:' + color + '">' + concept_text + \
                     '<div class="tooltiptext">' + semantic_type_info + '</div></div>'
            #sub_string = '<div class="tooltip" style="color:' + color + '">' + concept_text + '</div>'

            line = re.sub(pattern, sub_string, line)
            #line = line.replace(concept_text, sub_string)
        
        line = line.replace("\n", "<br>")
        report_text[idx] = line
        idx += 1

    print(stats)
    # build the annotated html file
    annotated_report = '<html>\n<head><link rel="stylesheet" href="../tooltip.css"></head>'  # put a simple header on for tooltips
    for rl in report_text:
        annotated_report += str(rl) 
    annotated_report = annotated_report + '<br><div class="statbox">' + stats + "</div><br>"

    color_map = ""
    for c in SEMANTIC_MAP:
        color = c["color"]
        name = c["name"]
        if color != "":
            color_map +=  name + ': <span style="color:' + color +'">' + color + '</span> &nbsp;&nbsp;| &nbsp;'
    annotated_report = annotated_report + '<br><div class="colorbox">' + color_map + "</div><br>"



    # cui_list = []
    # cui_info = ""
    # for c in concept_map:
    #     concept_text = c["matchedtext"]
    #     semantic_type = c["evlist"][0]["conceptinfo"]["semantictypes"][0]
    #     cmd = get_semantic_meta(semantic_type)        

    #     cui_info += '<b>concept:</b> {} <b>preferredname:</b> {} <b>cui:</b> {} <b>semantic_type:</b> {}<br>'.format(concept_text, c["evlist"][0]["conceptinfo"]["preferredname"], c["evlist"][0]["conceptinfo"]["cui"], cmd["name"])

    #annotated_report = annotated_report + '<br><div class="cuibox">' + cui_info + "</div><br></html>"

    annotated_report += "</html>"
    with open( out_file, 'w') as out:
        out.write(annotated_report)

    out.close()

   
if __name__ == "__main__":
    CONCEPT_FILE = ""
    
    parser = argparse.ArgumentParser()
    #parser.add_argument("-m", action="store_true", help="export field metadata")
    parser.add_argument("-i", help="input path to reports")
    parser.add_argument("-o", help="output path to annotated html report")
  
    args = parser.parse_args()

    if args.i:
        INPATH = args.i
    if args.o:
        OUTPATH = args.o

    SEMANTIC_MAP = open_semantic_map()

    for x in os.listdir(INPATH):
        if x.endswith(".txt"):
            split_tup = os.path.splitext(x)
            file_name = split_tup[0]
            concept_file = INPATH + file_name + ".json"    # assume the process file with concepts has corresponding json ext
            report_file = INPATH + x
            out_file = OUTPATH + file_name + ".html"

            print('report: {} concepts: {} annotated: {}'.format(report_file, concept_file, out_file))
            generate_annotation(concept_file, report_file, out_file)


 

    