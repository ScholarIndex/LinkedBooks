import ipdb as pdb
import pytest
from pytest import mark
#from content_ingester import ingest_content_LO10015953
import logging
import os
import json
from random import shuffle
import multiprocessing as mp
from mongoengine import connect
from content_ingester.content_ingestion import process_monograph_folder
from content_ingester.content_ingestion import detect_monographs
from content_ingester.content_ingestion import save_monograph_to_db
from commons.dbmodels import Page, LBDocument

logger = logging.getLogger(__name__)


@mark.run(order=4)
def test_content_ingestion_journals(mongoengine_connection, data_dir="tests/test_data/content/journals/"):
    """
    Test ingestion of journal contents (except ArchivioVeneto, see below).
    """
    detected_issues = detect_issues(data_dir)
    [logger.info("Found issue to ingest: %s"%issue) for issue in detected_issues]
    issue_objects = [process_journalissue_folder(issue) for issue in detected_issues]
    results = [(issue.issue_number, save_journalissue_to_db(issue)) for issue in issue_objects]
    logger.info(results)
    for issue_number, outcome in results:
        assert outcome is not False
    docs_succeeded = len([outcome for issue_number, outcome in results if outcome])
    docs_failed = len([outcome for issue_number, outcome in results if outcome is False])
    docs_skipped = len([outcome for issue_number, outcome in results if outcome is None])
    logger.info("Finished ingestion. %i documents were ingested; %i failed; %i skipped."%(docs_succeeded
                                                                            , docs_failed
                                                                            , docs_skipped))
    return


@mark.run(order=5)
def test_content_ingestion_journals_force_update(mongoengine_connection, data_dir="tests/test_data/content/journals/"):
    """
    Test ingestion of journal contents (except ArchivioVeneto, see below).
    """
    pages = list(Page.objects)
    shuffle(pages)
    # add some pages to the golden set for the sake of the test
    for page in pages[:5]:
        page.in_golden = True
        page.save()
    logger.info("Page added to golden set: %s"%page.id)
    detected_issues = detect_issues(data_dir)
    [logger.info("Found issue to ingest: %s"%issue) for issue in detected_issues]
    issue_objects = [process_journalissue_folder(issue) for issue in detected_issues]
    results = [
        (
            issue.issue_number,
            save_journalissue_to_db(issue, force_update=True)
        )
        for issue in issue_objects
    ]
    logger.info(results)
    for issue_number, outcome in results:
        assert outcome is not False
    docs_succeeded = len([outcome for issue_number, outcome in results if outcome])
    docs_failed = len([outcome for issue_number, outcome in results if outcome is False])
    docs_skipped = len([outcome for issue_number, outcome in results if outcome is None])
    logger.info("Finished ingestion. %i documents were ingested; %i failed; %i skipped."%(docs_succeeded
                                                                            , docs_failed
                                                                            , docs_skipped))
    return


@mark.run(order=6)
@pytest.mark.skip
def test_content_ingestion_LO10015953(test_db, data_dir="tests/test_data/content/LO10015953/"):
    """
    Test ingestion of issues of _Archivio Veneto_ (bid="LO10015953")
    """
    bid = "LO10015953"
    basedir = data_dir
    issues = [{"number":result,"path":"%s%s/"%(basedir,result)}
     for result in os.listdir(basedir)
     if os.path.isdir("%s%s"%(basedir,result))]
    print(issues)
    bid_metadata_id = test_db.metadata.find_one({"bid":bid},{"_id":1})["_id"]
    for issue in issues:
        document = ingest_content_LO10015953.process_folder(
            issue["path"],
            bid,
            bid_metadata_id
        )
    ingest_content_LO10015953.save_document_to_db(document,test_db)


@mark.run(order=7)
def test_content_ingestion_monographs(
    mongoengine_connection,
    data_dir="tests/test_data/content/books/"
):
    """
    Test ingestion of monograph content.
    """
    detected_monographs = detect_monographs(data_dir)
    [
        logger.info("Found monographs to ingest: %s" % monograph)
        for monograph in detected_monographs
    ]
    logger.debug(detected_monographs)
    monograph_objects = [
        process_monograph_folder(monograph)
        for monograph in detected_monographs
    ]
    results = []
    for monograph in monograph_objects:
        if monograph.pages == []:
            results.append((monograph.bid, False))
        else:
            results.append((
                monograph.bid,
                save_monograph_to_db(monograph, force_update=True)
            ))
    docs_succeeded = [
        bid
        for bid, outcome in results if outcome
    ]

    docs_failed = [
        bid
        for bid, outcome in results
        if outcome is False
    ]

    docs_skipped = [
        bid
        for bid, outcome in results
        if outcome is None
    ]

    logger.info(
        "Finished: {} documents ingested; {} failed; {} skipped.".format(
            len(docs_succeeded),
            len(docs_failed),
            len(docs_skipped)
        )
    )
    logger.info("Documents succeeded {}".format(docs_succeeded))
    logger.info("Documents failed {}".format(docs_failed))
    logger.info("Documents skipped {}".format(docs_skipped))


@mark.run(order=8)
@pytest.mark.skip(reason="Need to figure out how mongoengine reverse delete rule works...")
def test_document_deletion(test_db):
    document = LBDocument.objects.first()
    logger.info("Document to delete %s, with %i pages" % (document.id, len(document.pages)))
    n_pages_pre = Page.objects.count()
    logger.info("Total pages before deletion: %i" % n_pages_pre)
    document.delete()
    n_pages_post = Page.objects.count()
    logger.info("Total pages after deletion: %i" % n_pages_post)
    assert n_pages_post < n_pages_pre
