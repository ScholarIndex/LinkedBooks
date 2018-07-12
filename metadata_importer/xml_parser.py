# -*- coding: utf-8 -*-
"""
Parser that extracts the folder structure in the master remote storage and keeps only xml (catalogue) files.
It produces a csv file of report, which can be checked in order to integrate missing xml files, and a folder with the structure of the
xml materials, plus adding the provenance in all xml files. This folder structure will be used by the next step of the metadata_importer
"""
__author__ = """Giovanni Colavizza"""

import os, codecs, csv, logging, argparse
logging.basicConfig(filename="logs/xml_parser.log", level=logging.INFO)
from bs4 import BeautifulSoup
from supporting_functions import walklevel

def xml_parser(in_folder,out_folder,csv_file):
    """
    Parser of the Linked Books storage, in order to extract xml metadata files and generate a csv_report of all volumes with missing xml file, without images, incorrectly containing folders or with too many xml files (more than 1 per level).

    :param in_folder: Folder pointing to the Linked Books main storage.
    :param out_folder: Folder where xml files are stored, mimicking the structure of the storage. This Folder is NOT overwritten at every run, but simply updated.
    :param csv_file: CSV file where problems are reported for further action. This file is overwritten at every run.
    :return: None.
    """

    # basic checks
    assert os.path.isdir(in_folder)
    if not os.path.isdir(out_folder):
        try:
            os.makedirs(out_folder, exist_ok=True)
        except:
            logging.warning("Unable to create the out_folder: %s"%out_folder)
    assert os.path.isdir(out_folder)
    logging.info("Initial checks OK.")

    with codecs.open(csv_file, "w", "utf-8") as f:
        csv_writer = csv.writer(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_NONE)
        csv_writer.writerow(["bid", "typology", "directory", "has_images", "has_many_xml", "has_subfolders", "has_meta", "provenance"])

        for root, dirs, files, level in walklevel(in_folder, 3):
            for d in dirs:
                if level == 2:
                    has_meta_bid = False
                    if len(d.split("_")) == 2:  # check provenance info is there
                        provenance, bid = d.split("_")
                    else:
                        continue
                elif level == 3:
                    has_meta_issue = False
                    if len(root.split("/")[-1].split("_")) == 2:  # check provenance info is there
                        provenance, bid = root.split("/")[-1].split("_")
                    else:
                        continue
                else:
                    continue
                for fil in os.listdir(os.path.join(root,d)):
                    if ".xml" in fil:
                        if level == 2:
                            has_meta_bid = True
                        if level == 3:
                            has_meta_issue = True
                        # read metadata and add provenance
                        try:
                            metadata = codecs.open(os.path.join(root, os.path.join(d,fil)), "r", "utf-8").read()
                        except:
                            logging.warning("Encoding error in %s"%(d+fil))
                        soup = BeautifulSoup(metadata, "html.parser")
                        if soup.find("provenance"): # if provenance is already there, just overwrite it
                            soup.find("provenance").string = provenance
                        else:
                            pr_tag = soup.new_tag("provenance")
                            pr_tag.string = provenance
                            if len(soup.find_all("dc")) > 0:
                                soup.dc.append(pr_tag)
                            else:
                                soup.append(pr_tag)
                        # make new dir and store new metadata in out_folder
                        new_dir = root.replace(in_folder,out_folder)
                        new_dir = os.path.join(new_dir,d)
                        if not os.path.isdir(new_dir):
                            try:
                                os.makedirs(new_dir, exist_ok=True)
                            except:
                                logging.warning("Unable to create the new_dir: %s" % new_dir)
                        try:
                            file_out = fil
                            if level == 2:
                                file_out = bid + ".xml"
                            f_out = open(os.path.join(new_dir, file_out), "wb")
                            f_out.write(soup.encode('utf-8', formatter="minimal"))
                            f_out.close()
                            logging.info("Wrote the xml file for: %s" % new_dir)
                        except:
                            logging.warning("Unable to write the xml file for: %s" % new_dir)
                # report problematic cases in csv files
                # check the presence of subfolders
                has_subfolders = False
                if len([x for x in os.listdir(os.path.join(root, d)) if os.path.isdir(os.path.join(root, d, x))]) > 0:
                    has_subfolders = True
                # check the presence of several metadata files per folder
                has_many_xml = False
                if len([x for x in os.listdir(os.path.join(root, d)) if ".xml" in x]) > 1:
                    has_many_xml = True
                # check the presence of images
                has_images = False
                if len([x for x in os.listdir(os.path.join(root, d)) if ".jpg" in x]) > 0:
                    has_images = True
                if level == 2:
                    type_doc = "book"
                    if "journals" in root:
                        type_doc = "journal"
                    if ((type_doc == "book") and has_subfolders) or has_many_xml or ((type_doc == "book") and not has_images) or not has_meta_bid:
                        csv_writer.writerow([bid,type_doc,d,has_images,has_many_xml,has_subfolders,has_meta_bid,provenance])
                elif level == 3:
                    type_doc = "issue"
                    if has_subfolders or has_many_xml or not has_images or not has_meta_issue:
                        csv_writer.writerow([bid,type_doc,d,has_images,has_many_xml,has_subfolders,has_meta_issue,provenance])

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Parser the Linked Books storage and exports xml metadata files.')
    parser.add_argument('--in_folder', dest='in_folder',
                        help='The root folder of the Linked Books storage.')
    parser.add_argument('--out_folder', dest='out_folder', nargs='?', default="xml_output",
                        help='The folder where xml files are to be stored.')
    parser.add_argument('--csv_file', dest='csv_file', nargs='?', default="xml_parser_out.csv",
                        help='The CSV file where the result of parsing is saved.')

    args = parser.parse_args()

    xml_parser(args.in_folder, args.out_folder, args.csv_file)