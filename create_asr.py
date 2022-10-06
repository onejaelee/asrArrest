# -*- coding: utf-8 -*-
"""
Created on Sat Feb 27 16:54:32 2021

@author: One Jae
"""
import os
import zipfile
import requests
import pandas as pd
url = "https://s3-us-gov-west-1.amazonaws.com/cg-d4b776d0-d898-4153-90c8-8336f86bdfec/masters/asr/"
dropbox = os.path.expanduser("~/Dropbox/Unfundedpension/src/int/")
raw = os.path.expanduser("~/Dropbox/Unfundedpension/src/raw/")

if not os.path.exists(raw + 'asr/'):
    os.makedirs(raw + 'asr/')
if not os.path.exists(dropbox + 'asr/'):
    os.makedirs(dropbox + 'asr/')
download_dir = raw + 'asr/'


def asr_to_df_totals(file_name):
    print(file_name)
    f = open(file_name, "r")
    
    m_state = []
    m_ori = []
    m_group = []
    m_div = []
    m_month = []
    m_year = []
    m_breakdown = []
    m_are = []
    m_zero = []
    m_lupdate = []
    m_pupdate =[]
    m_ppupdate =[]
    m_jdep = []
    m_jcourt = []
    m_jwelfare = []
    m_jpolice = []
    m_jcriminal = []
    
    d_state = []
    #d_ori = []
    #d_group = []
    #d_div = []
    #d_month = []
    #d_year = []
    d_off = []
    
    h_msa = None
    h_county = None
    h_seq = None
    h_suburb = None
    h_corecity = None
    h_covered = None
    h_pop = None
    h_name = None
    h_state_name = None
    m_state = None
    m_ori = None
    m_group = None
    m_div = None
    m_month = None
    m_year = None
    m_breakdown = None
    m_are = None
    m_zero = None
    m_lupdate = None
    m_pupdate = None
    m_ppupdate = None
    m_jdep = None
    m_jcourt = None
    m_jwelfare = None
    m_jpolice = None
    m_jcriminal = None

    rows = {}
    for x in["Agency Name","Month", "Year",'State', 'ORI', 'Group', "Division", 
             "Breakdown", "ARE",
            "Zero", "Last Update", "First Prev Update", "Second Prev Update",
            "Juvenile Disposition Indicator","Juvenile Disposition Juvi",
            "Juvenile Disposition Welfare", "Juvenile Disposition Police",
            "Juvenile Disposition Criminal", "MSA", "County", "Sequence Number", 
            "Suburban", "Core City", "Covered By", "Population",
            "State Name", "State Code", "ORI Code", "Offense Code", "Count"]:
        rows[x] = []
    
            
    for i, line in enumerate(f):
        if line[0] != '3':
            continue
        #this means its an agency header
        if line[13:15] == '00':
            h_msa = line[17:20]
            h_county = line[20:23]
            h_seq = line[23:28]
            h_suburb = line[28]
            h_corecity = line[29]
            h_covered = line[30]
            h_pop = line[31:40]
            h_name = line[40:64]
            h_state_name = line[64:70]
        #this means its a monthly header
        elif line[17:20] == '000':
            m_state = line[1:3]
            m_ori = line[3:10]
            m_group = line[10:12]
            m_div = line[12]
            m_month = line[13:15]
            m_year = line[15:17]
            m_breakdown = line[20]
            m_are = line[21]
            m_zero = line[23]
            m_lupdate = line[24:31]
            m_pupdate = line[31:38]
            m_ppupdate = line[38:45]
            m_jdep = line[45]
            m_jcourt = line[46:51]
            m_jwelfare = line[51:56]
            m_jpolice = line[61:66]
            m_jcriminal = line[66:71]
        else:
            d_state = line[1:3]

            d_off=line[17:20]
            
            occ = line[20:23]

            shift = 0
            
            row = {"Agency Name":h_name, "Month":m_month,"Year":m_year,'State': m_state,  'Group':m_group, "Division":m_div, 
             "Breakdown":m_breakdown, 
            "Zero":m_zero, 
 
            "MSA":h_msa, "County":h_county, "Sequence Number":h_seq, 
            "Suburban":h_suburb, "Core City":h_corecity, "Covered By":h_covered, "Population":h_pop,
            "State Name":h_state_name, "State Code":d_state, 'ORI':m_ori, "Offense Code":d_off, "Count": 0}
            for x in range(int(occ)):
                if line[23+shift:26+shift] not in ['111','222','333','444', '\n', '']:
                    #For some reason, some counts are listed as "0000J" and various other letters
                    try:
                        row["Count"] += int(line[26+shift:31+shift])
                    except ValueError:
                        
                        
                        if m_state == '06':
                            if m_group.replace(" ", "") in ['1A','1B','1C','2','3','4','5','6','7']:
                                print(m_state, m_year, h_name, m_month, line[26+shift:31+shift],line[17:20])
                            else:
                                print('CAUGHT', m_state, m_year, h_name, m_month, line[26+shift:31+shift],line[17:20])

                shift += 8
            for key in rows.keys():
                if key in row:
                    rows[key].append(row[key]) 
                else:
                    rows[key].append(0)
    rows = pd.DataFrame(rows)

    return rows
        
def asr_to_df(file_name):
    print(file_name)
    f = open(file_name, "r")
    
    m_state = []
    m_ori = []
    m_group = []
    m_div = []
    m_month = []
    m_year = []
    m_breakdown = []
    m_are = []
    m_zero = []
    m_lupdate = []
    m_pupdate =[]
    m_ppupdate =[]
    m_jdep = []
    m_jcourt = []
    m_jwelfare = []
    m_jpolice = []
    m_jcriminal = []
    
    d_state = []
    d_ori = []
    d_group = []
    d_div = []
    d_month = []
    d_year = []
    d_off = []
    
    h_msa = None
    h_county = None
    h_seq = None
    h_suburb = None
    h_corecity = None
    h_covered = None
    h_pop = None
    h_name = None
    h_state_name = None
    m_state = None
    m_ori = None
    m_group = None
    m_div = None
    m_month = None
    m_year = None
    m_breakdown = None
    m_are = None
    m_zero = None
    m_lupdate = None
    m_pupdate = None
    m_ppupdate = None
    m_jdep = None
    m_jcourt = None
    m_jwelfare = None
    m_jpolice = None
    m_jcriminal = None

    rows = {}
    for x in['State', 'ORI', 'Group', "Division", 
            "Month", "Year", "Breakdown", "ARE",
            "Zero", "Last Update", "First Prev Update", "Second Prev Update",
            "Juvenile Disposition Indicator","Juvenile Disposition Juvi",
            "Juvenile Disposition Welfare", "Juvenile Disposition Police",
            "Juvenile Disposition Criminal", "MSA", "County", "Sequence Number", 
            "Suburban", "Core City", "Covered By", "Population",
            "Agency Name","State Name", "State Code", "ORI Code"]:
        rows[x] = []
    for x in range(1,57):
        x = str(x)
        if len(x) == 1:
            rows['00'+x] = [] 
        else:
            rows['0'+x] = []
    
            
    for i, line in enumerate(f):
        if line[0] != '3':
            continue
        #this means its an agency header
        if line[13:15] == '00':
            h_msa = line[17:20]
            h_county = line[20:23]
            h_seq = line[23:28]
            h_suburb = line[28]
            h_corecity = line[29]
            h_covered = line[30]
            h_pop = line[31:40]
            h_name = line[40:64]
            h_state_name = line[64:70]
        #this means its a monthly header
        elif line[17:20] == '000':
            m_state = line[1:3]
            m_ori = line[3:10]
            m_group = line[10:12]
            m_div = line[12]
            m_month = line[13:15]
            m_year = line[15:17]
            m_breakdown = line[20]
            m_are = line[21]
            m_zero = line[23]
            m_lupdate = line[24:31]
            m_pupdate = line[31:38]
            m_ppupdate = line[38:45]
            m_jdep = line[45]
            m_jcourt = line[46:51]
            m_jwelfare = line[51:56]
            m_jpolice = line[61:66]
            m_jcriminal = line[66:71]
        else:
            d_state = line[1:3]

            occ = line[20:23]

            shift = 0
            
            row = {'State': m_state, 'ORI':m_ori, 'Group':m_group, "Division":m_div, 
            "Month":m_month, "Year":m_year, "Breakdown":m_breakdown, "ARE":m_are,
            "Zero":m_zero, "Last Update":m_lupdate, "First Prev Update":m_pupdate, "Second Prev Update":m_ppupdate,
            "Juvenile Disposition Indicator":m_jdep,"Juvenile Disposition Juvi":m_jcourt,
            "Juvenile Disposition Welfare":m_jwelfare, "Juvenile Disposition Police":m_jpolice,
            "Juvenile Disposition Criminal":m_jcriminal, "MSA":h_msa, "County":h_county, "Sequence Number":h_seq, 
            "Suburban":h_suburb, "Core City":h_corecity, "Covered By":h_covered, "Population":h_pop,
            "Agency Name":h_name,"State Name":h_state_name, "State Code":d_state, "ORI Code":d_ori}
            
            for x in range(int(occ)):
                if line[23+shift:26+shift] not in ['111','222','333','444', '\n', '']:
                    row[line[23+shift:26+shift]] = line[26+shift:31+shift]

                shift += 8
            for key in rows.keys():
                if key in row:
                    rows[key].append(row[key]) 
                else:
                    rows[key].append(0)
    print(len(rows.keys()))
    rows = pd.DataFrame(rows)
    print(rows)

    return rows



if __name__ == '__main__':
    
    
    
    
    
    years = []
    files = []
    for x in range(1985,2020):
        file_name ="asr-" + str(x) + ".zip"
        if not os.path.exists(raw + "asr-" + str(x)):
            if not os.path.exists(raw + file_name):
                name = url + file_name
                r = requests.get(name, allow_redirects=False)
                with open(download_dir + file_name, 'wb') as f:
                    f.write(r.content)
        
            with zipfile.ZipFile(download_dir + file_name,"r") as zip_ref:
                zip_ref.extractall(download_dir)
            os.remove(download_dir + file_name)
                
            for filename in os.listdir(download_dir + "asr-" + str(x)):
                if (filename.endswith(".zip") or filename.endswith(".ZIP")) and "mth" in filename: 
                    with zipfile.ZipFile(download_dir + "asr-" + str(x) +'/'+ filename,"r") as zip_ref:
                        zip_ref.extractall(download_dir + "asr-" + str(x))
                    continue
                else:
                    continue
        for filename in os.listdir(download_dir + "asr-" + str(x)):
            if (filename.endswith(".DAT") or filename.endswith(".dat") or filename.endswith(".txt") or filename.endswith(".TXT")) and x <= 2014:
                files.append(filename)
                years.append(asr_to_df_totals(download_dir + "asr-" + str(x) + '/' + filename))
            elif filename.endswith(".txt") or filename.endswith(".DAT") or filename.endswith(".TXT"):

                if (("KCA" in filename  or "COMB" in filename  or "ASR2016" in filename) and "2015_ASRMNTH_NATIONAL_MASTER_FILE" not in filename) or "2017_ASR1MON_NATIONAL_MASTER_FILE" in filename or "2018_ASR1MON_NATIONAL_MASTER_FILE" in filename or "2019_ASR1MON_NATIONAL_MASTER_FILE_STATIC" in filename:
                    files.append(filename)
                    years.append(asr_to_df_totals(download_dir + "asr-" + str(x) + '/' + filename))

    print(files)

    years = pd.concat(years, ignore_index=True)
    years.to_csv(dropbox + '/asr/asr.csv',index=False)
    years.to_pickle(dropbox + '/asr/asr.pkl')

                    