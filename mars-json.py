"""
@summary: Converts a MARS formatted report (text) to a JSON formatted file.  This is based on GIant code.
"""
import os
import sys
import argparse
import csv
import datetime as dt
import logging
import random
import re
import types
import warnings
import json
#import settings

infoLogger = logging.getLogger('info')
infoLogger.setLevel(logging.INFO)
errorLogger = logging.getLogger('error')
errorLogger.setLevel(logging.WARNING)

formatter = logging.Formatter('%(asctime)s: %(funcName)s - %(levelname)s - %(message)s')

infoFileHandler = logging.FileHandler(filename=os.path.join(os.pardir, os.pardir, 'logs', 'giant_reports_{0}.log'.format(dt.date.today())))
infoFileHandler.setFormatter(formatter)
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(formatter)
fileHandler = logging.FileHandler(filename=os.path.join(os.pardir, os.pardir, 'logs', 'giant_reports_errors_{0}.log'.format(dt.date.today())))
fileHandler.setFormatter(formatter)

infoLogger.addHandler(infoFileHandler)
#errorLogger.addHandler(consoleHandler)
errorLogger.addHandler(fileHandler)

class RawReportManager:
    ##Open and parse a report file.  Generate a list of RawReport objects.
    ##Database connection info is deprecated in favor of using the Django ORM,
    ##which can be set to use the test database (giantdev.dbmi.pitt.edu) or
    ##the production database (elmer.dbmi.pitt.edu)
    def __init__(self, filename, ignore=False, **kwargs):
        if ignore:
            self.warn()
        self.filename = filename
        self.text = self.readRawReports(filename)
        self.ignore = ignore
        if 'reports' in kwargs:
            self.reports = kwargs['reports'] 
        else:
            infoLogger.info('Processing file {0}'.format(filename))
            self.makeRecords()
        
    ##Add a report to the list self.reports
    def add_report(self, report):
        self.reports.append(report)
        
    ##Make sure all reports have unique checksums
    def check_checksums(self):
        checksums = set()
        for report in self.reports:
            checksums.add(report.checksum)
        if len(checksums) != len(self.reports) and self.ignore is False:
            raise RawReportManagerException("Some checksums not unique: {0} checksums, {1} reports".format(len(checksums), len(self.reports)))
       
    ##Some sanity checks for a provided separate RawReportManager instance
    ##to show that they are probably made up of the same reports
    def _check_manager(self, other):
        if len(self.reports) != len(other.reports):
            raise RawReportManagerException("Length of reports in {0} does not match that of {1}, {2} != {3}".format(self, other, len(self.reports), len(other.reports)))
    
    ##Validate a collection of django model objects, then save
    ##them all to the database
    def clean_and_save(self, modelInstances):
        for instance in modelInstances:
            instance.full_clean()
        for instance in modelInstances:
            instance.save()
    
    ##Try to discover if the report file at filename
    ##contains some extraneous headers
    def find_header_errors(self, filename):
        f = open(filename)
        sohFlag = 0
        eorFlag = 0
        ctr = 0
        for line in f:
            ctr += 1
            if "S_O_H" in line:
                if not eorFlag:
                    print ('Header error line: ' + str(ctr))
                sohFlag = 1
                eorFlag = 0
            if "E_O_R" in line:
                if not sohFlag:
                    print ('Header error line: ' + (ctr))
                eorFlag = 1
                sohFlag = 0 
    
    ##Get reports that have duplicate attr
    def get_duplicates(self, attr):
        attrset = {}
        dups = {}
        for report in self.reports:
            reportattr = getattr(report, attr)
            if reportattr in attrset:
                if reportattr in dups:
                    dups[reportattr].append(report)
                else:
                    dups[reportattr] = [attrset[reportattr], report]
            else:
                attrset[reportattr] = report
        return dups
    
  
    ##Returns a subset of reports given a list of report
    ##ids
    def get_reports(self, request):
        result = []
        for rep in self.reports:
            if rep.reportid in request:
                result.append(rep)
        return result
        
    ##Returns the unique reports types found in self.reports
    def get_report_types(self):
        reportTypes = set()
        for r in self.reports:
            reportTypes.add(r.recordtype)
        return reportTypes
    
    def has_report(self, checksum):
        """Check to see if a report with the given checksum is in self.reports"""
        for report in self.reports:
            if report.checksum == checksum:
                return True
        return False
                                      
    ##Split text string into a list of individual RawReport objects.
    ##Amasses errors thrown by RawReport and raises an exception if any are found.
    def makeRecords(self):
        self.reports = []
        soh = re.compile(r'S_O_H\r?\n')
        eor = re.compile(r'E_O_R\r?\n')
        startPts = [s.start() for s in soh.finditer(self.text)]
        endPts = [e.end() for e in eor.finditer(self.text)]
        errors = []
        if len(startPts) == len(endPts):
            for i in range(len(startPts)):
                try:
                    self.add_report(RawReport(self.text[startPts[i]:endPts[i]+1], self.ignore))
                except RawReportManagerException as err:
                    if isinstance(err.value, list):
                        errors.extend(err.value)
                    else:
                        errors.append(err.value)
        else:
            raise ValueError(str(len(startPts)) + " S_O_H tags, " + str(len(endPts)) + " E_O_R tags")
        if errors:
            print ("Errors while reading {0} reports:".format(len(self.reports) + len(errors)))
            for error in errors:
                errorLogger.error(error)
            if self.ignore is False:
                raise RawReportManagerException(errors, types=True)
                                               
    ##Count the unique occurrences of report.attr for this set of reports
    ##(e.g. attr='patientid' to count number of patients)
    def count(self, attr):
        attrCount = set()
        for report in self.reports:
            attrCount.add(getattr(report,attr))
        return len(attrCount)

    ##Read report text file into a string
    def readRawReports(self, reportsFile):
        """reads in the text file"""
        f=open(reportsFile, 'r')
        text=f.read()
        f.close()
        return text  #.decode('cp1252')
           
    def warn(self):
        """Warn the user if errors are being ignored"""
        userInput = input("You are ignoring errors, continue? y/[n]")
        if userInput in ["n", ""]:
            sys.exit(1)
        elif userInput in ["y", "yes"]:
            pass
        else:
            self.warn()
               
    #@transaction.commit_on_success()
    def write_report_metadata(self, attributes):
        """Save some extra data about the reports in the ReportMetadata table
        
        Arguments:
            attribute - an attribute present on the RawReport objects
        """
        for rawreport in self.reports:
            report = Report.objects.get(checksum=rawreport.checksum)
            for attribute in attributes:
                value = getattr(rawreport, attribute.lower().replace(" ", "")).strip()
                try:
                    metadata = ReportMetadata.objects.get(report=report, attribute=attribute, value=value)
                except ReportMetadata.DoesNotExist:
                    try:
                        ReportMetadata(report=report, attribute=attribute, value=value).save()
                    except IntegrityError as err:
                        errorLogger.error(err)
                        print (err)
        
    def __str__(self):
        return "RawReportManager: {0}".format(os.path.basename(self.filename))

class RawReportManagerException(Exception):
    def __init__(self, value, types=False):
        self.types = types
        self.value = value
    
    def __str__(self):
        if self.types:
            return "\n".join(set([x.type for x in self.value]))
        if isinstance(self.value, list):
            return "\n".join([str(error) for error in self.value])
        else:
            return repr(self.value)

class RawReportException(Exception):
    def __init__(self, type, case):
        self.type = type
        self.case = case
        self.value = " ".join([type, case])
        
    def __str__(self):
        return self.value
        
    

class RawReport:
    """RawReport attributes:
        reportid (Report.reportID)
        patientid (may be from mainpatientidentifier, Report.ptID)
        checksum (Report.checksum)
        date (may be from principaldate, Report.date)
        time (may be from principaldate, Report.time)
        recordtype (maps to Report.rType)
        subtype (Record.subtype)
        facility (Record.facility)
    """
    ##Save report text, get access to header info.
    ##Set and clean attributes.
    def __init__(self, text, ignore=False):
        self.ignore = ignore
        self.text = text
        self.header, self.values = self.get_header()
        for i in range(len(self.header)):
            setattr(self, self.header[i].lower().replace(" ", ""), self.values[i])
        self.clean_attrs()
    
    ##Using a dictionary of name:value pairs, add 
    ##these to the header text
    def add_headers(self, headers):
        lines = self.text.splitlines()
        soh = self.get_first_line_containing(lines, "S_O_H")
        for heading, value in headers.items():
            lines[soh+1] += "\t{0}".format(heading)
            lines[soh+2] += "\t{0}".format(value)
        self.text = "\n".join(lines)
        
    def change_header(self, header, value):
        lines = self.text.splitlines()
        soh = self.get_first_line_containing(lines, "S_O_H")
        headerFields = lines[soh+1].split("\t")
        values = lines[soh+2].split("\t")
        i = headerFields.index(header)
        values[i] = value
        lines[soh+2] = "\t".join(values)
        self.text = "\n".join(lines)
                  
    ##Return the index of the first line containing str in a list of lines
    def get_first_line_containing(self, lines, str):
        for line in lines:
            if str in line:
                return lines.index(line)
        return None
    
    ##Sanity checks on a date string
    def is_date(self, date):
        dsplit = date.split()
        if len(dsplit) == 1:
            return self.check_date(date)
        elif len(dsplit) == 2:
            return self.check_date(dsplit[0])
        return False
    
    def check_date(self, date):
        if len(date) != 8:
            return False
        for char in date:
            if not char.isdigit():
                return False
        return True
    
    ##Make sure the parsed report contains the data needed to insert it
    ##in the database
    def clean_attrs(self):
        errors = []
#         if hasattr(self,'mainpatientidentifier'):
#             mpSplit = self.mainpatientidentifier.split()
#             if len(mpSplit) > 1:
#                 if not self.is_mrn(mpSplit[0]):
#                     self.patientid = mpSplit[0]
#                 else:
#                     self.patientid = mpSplit[2]
#                 self.fix_mpi_in_text()
#             else: 
#                 self.patientid = self.mainpatientidentifier
#         if self.is_mrn(self.patientid):
#             errors.append(RawReportException("Patient ID is an MRN", str(self.patientid)))
        if not hasattr(self,'checksum'):
            print (self.header)
            print (self.values)
            errors.append(RawReportException("No checksum", str(self.reportid)))
        if hasattr(self,'date'):
            self.rawdate = self.date
            if self.is_date(self.date):
                self.date = self.fixDate(self.date)
            else:
                errors.append(RawReportException("Data in date field is not a date" "Report: {0}, Date: {1}".format(self.checksum, self.date)))
        else:
            if hasattr(self,'principaldate'):
                if self.is_date(self.principaldate):
                    self.rawdate = self.principaldate
                    self.date = self.fixDate(self.principaldate)
                else:
                    errors.append(RawReportException("Data in date field is not a date" "Report: {0}, Date: {1}".format(self.checksum, self.principaldate)))
            else:
                errors.append(RawReportException("No date in report", str(self.checksum)))
        if not hasattr(self,'time'):
            self.time = "00:00:00"
            if hasattr(self,'rawdate'):
                dateSplit = self.rawdate.split(" ")
                if len(dateSplit) == 2:
                    try:
                        self.time = self.fixTime(dateSplit[1])
                    except ValueError:
                        pass
        if not hasattr(self,'subtype'):
            self.subtype = 'unk'
        if not hasattr(self,'facility'):
            self.facility = 'unk'
        if not hasattr(self,'recordtype'):
            errors.append(RawReportException("No recordtype found for report", str(self.checksum)))
        if errors and self.ignore is False:
            raise RawReportManagerException(errors)
            
    ##Clean up main patient identifiers within the report 
    ##text itself
    def fix_mpi_in_text(self):
        lines = self.text.splitlines()
        for line in lines:
            if "Main Patient Identifier" in line:
                header = line.split("\t")
                vals = lines[lines.index(line)+1].split("\t")
                mpi = vals[header.index("Main Patient Identifier")].split()
                if len(mpi) == 3:
                    id = mpi[0]
                    if not self.is_mrn(id):
                        vals[header.index("Main Patient Identifier")] = id
                        lines[lines.index(line)+1] = "\t".join(vals)
                    self.text = "\n".join(lines)
                break     
    
    ##Deprecated: Generate a random checksum if needed
    def generate_checksum(self):
        warnings.warn("Deprecated", DeprecationWarning)
        return self.reportid.replace(",","-") + "-" + str(random.randint(0,100000000))
    
    ##Return the header headings and values for self.text
    def get_header(self):
        headerText = self.text[self.text.find("S_O_H") + 5:self.text.find("E_O_H")].strip().split("\n")
        types = headerText[0].split("\t")
        values = headerText[1].split("\t")
        if len(types) > len(values) :
            while len(types) > len(values):
                values.append("")
        elif len(types) < len(values):
            raise ValueError("More types than values in header: \n" + str(headerText))
        return [x.strip() for x in types], [x.strip() for x in values]
    
    ##Conversions for MARS data
    def fixDate(self, date):
        """converts MARS formated date in python date object"""
        date=(int(date[0:4]),int(date[4:6]),int(date[6:8]))
        newDate=dt.date(date[0],date[1],date[2])
        return str(newDate)

    def fixTime(self, time):
        """converts MARS formated time in python time object"""
        time=(int(time[0:2]),int(time[2:4]))
        newTime=dt.time(time[0],time[1])
        return str(newTime)
    
    ##Check if main patient id is an MRN
    def is_mrn(self, id):
        for char in id:
            if not char.isdigit():
                return False
        return True
    
    ##Deprecated: It's possible to parse self.recordtype from the checksum,
    ##            but it's better to resolve this during report generation
    def parse_type_from_checksum(self):
        warnings.warn("Deprecated", DeprecationWarning)
        section = self.checksum.split("-")[0]
        type = ""
        for char in section:
            if char.isalpha():
                type += char
        self.recordtype = type
        
    ##Return a Report object whose attributes have 
    ##been updated based on the RawReport instance
    def update_report(self, report):
        report.reportID = self.reportid
        report.ptID = Patient.objects.get(pk=self.patientid)
        report.checksum = self.checksum
        report.date = self.date
        report.time = self.time
        report.subtype = self.subtype  
        report.rType = self.recordtype
        report.facility = self.facility
        report.text = self.text
        return report
    
    def __eq__(self, other):
        return self.checksum == other.checksum
    
    def __ne__(self, other):
        return self.checksum != other.checksum
    
    def __str__(self):
        return " - ".join([self.reportid, self.checksum])

def reports_subset(filename, requests, outfile):    
    rm = RawReportManager(filename)
    reps = rm.get_reports(requests)
    out = ""
    for rep in reps:
        out += rep.text
    fw = open(outfile, 'w')
    fw.write(out)
    fw.close()
       

def write_to_json(reports, outfile):
    print("writing json to {}".format(outfile))
    jreport = {}
    areport = []
    for report in reports:
        #rpt = {}
        #rpt[report.reportid] = {"patient_id": report.patientid, 
        headerless_text = stripHeader(report.text)
        rpt = {"report_id":report.reportid, "patient_id": report.patientid, 
            "rsub_type": report.subtype, "rtype":report.recordtype, "rtext":headerless_text}
        #jreport.update(rpt)
        areport.append(rpt)
    #print(jreport)
    with open(outfile, 'w') as out:  
        json.dump(areport, out)

def show_headers(reports):
    print("showing report headers...")
    jreport = {}
    for report in reports:        
        rpt = {"patient_id": report.patientid, 
            "rsub_type": report.subtype, "rtype":report.recordtype}
        print(rpt)

def stripHeader(txt):
    idx = txt.find("E_O_H")
    if idx > -1:
        return txt[idx+5:]
    return txt

if __name__ == "__main__":
    reportPath = "E:/GIANT/Reports/94" 
    filename = "sample-mars.txt"
    SHOW_HEADERS = False

    parser = argparse.ArgumentParser()
    parser.add_argument("infile", help="MARS report file to convert to JSON")
    parser.add_argument("-headers",action="store_true", help="This option displays the headers only to screen")
    args = parser.parse_args()

    if args.infile:
        rawReportFile = args.infile
    if args.headers:
        SHOW_HEADERS = True

    #rawReportFile = os.path.join(reportPath, filename)
    rm = RawReportManager(rawReportFile)
    #print (rm.get_report_types())
    print ("Read a total {} reports ".format(len(rm.reports)))
    #print (rm.reports[0])
    #print (rm.reports[0].header)
    #print (rm.reports[0].patientid)
    #print (rm.reports[0].text)
    if SHOW_HEADERS:
        show_headers(rm.reports)
    else:        
        write_to_json(rm.reports, rawReportFile+".json")

    #rm.write_to_db(projectids, attributes)

