import os, pandas, csv, re
import numpy as np
import hashlib
from biothings.utils.dataload import dict_convert, dict_sweep
from biothings import config
logging = config.logger
def load_gnomad_genome(data_folder):
    infile = os.path.abspath("/opt/biothings/GRCh37/gnomAD_genomes/r2.1/GnomadGenomes.tsv")
    assert os.path.exists(infile)
    with open(infile) as fp:
        reader = csv.reader(fp, delimiter='\t')
        header = next(reader)
        for line in reader:
            rec = dict(zip(header,line))
            var = rec["release"] + "_" + str(rec["chromosome"]) + "_" + str(rec["position"]) + "_" + rec["reference"] + "_" + rec["alternative"]       
            _id = hashlib.sha224(var.encode('ascii')).hexdigest()       
            process_key = lambda k: k.replace(" ","_").lower()
            rec = dict_convert(rec,keyfn=process_key)
            rec = dict_sweep(rec,vals=[np.nan])
            results = {}
            results.setdefault(_id,[]).append(rec)
            for _id,docs in results.items():
                doc = {"_id": _id, "gnomad_genome" : docs}
                yield doc
