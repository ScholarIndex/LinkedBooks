from flask_restplus import Model, fields

stats_fields = Model('Statistic', {
    'id': fields.String(
        required=True,
        description='The statistic ID (e.g. `total_scans`)'
    ),
    'label': fields.String(
        required=True,
        description='The statistic\'s label (for display)'
    ),
    'value': fields.Integer(required=True, description='The actual value'),
})

stats_summary = Model('StatsSummary', {
    'stats': fields.List(fields.Nested(stats_fields, required=True, description="Basic statistics about the LinkedBooks database"))
})

reference_fields = Model('Reference', {
    'id': fields.String(required=True, description='The local ID (MongoID) of the reference'),
    'bid': fields.String(required=False, description='The bibliographic ID (BID) of the containing publication'),
    'reference_string': fields.String(required=True, description='The text segment constituing a bibliographic reference'),
    'document_id': fields.String(required=True, description='The local ID (MongoID) of the containing document (i.e. the digitized object)'),
    'type': fields.String(
        required=True,
        description='The type of reference (values: meta-annotation, primary,\
         secondary)',
        attribute='ref_type'),
    'start_img_number': fields.Integer(required=True, description='Starting number of digitized page image'),
    'end_img_number': fields.Integer(required=True, description='Ending number of digitized page image'),
    'snippet': fields.String(required=True, description='Text snippter surrounding the reference'),
    'containing_document_type': fields.String(required=False, description='The type of containing publication (values: article, book)'),
    'containing_document_id': fields.String(required=False, description='The local ID (MongoID) of the containing publication (bibliographic object)'),
})

hierarchy_level_fields = Model('HierarchyLevel', {
    'id': fields.String(
        required=True,
        description='The local ID (MongoID) of the primary source'
    ),
    'document_type': fields.String(
        required=True,
        description='Type of archival record (e.g. archivio, fondo, serie)'
    ),
    'level': fields.Integer(
        required=True,
        description='The hierarchical level of a primary source'
    ),
    'current': fields.Boolean(
        required=True,
        description='Whether the primary source is the one targeted by the\
         bibliographic reference'
    ),
    'title': fields.String(
        required=True,
        description='Label to display for the primary source'
    ),
    'internal_id': fields.String(
        required=True,
        description='The primary source\'s internal id'
    )
})

primary_source_fields = Model('PrimarySource', {
    'id': fields.String(required=True, description='The local ID (MongoID) of the primary source'),
    'archive': fields.String(required=False, description='The archive (e.g. AsVe)'),
    'type': fields.String(required=True, description='The type of primary source (e.g. fund, series, etc.)', attribute="document_type"),
    'label': fields.String(required=True, description='A label for display, with information about the hierarchy'),
    'internal_id': fields.String(required=True, description='URN-like internal ID'),
    'link': fields.String(required=True, description='Link to the primary source page on the archive\'s website (if available)', attribute="url"),
    'incoming_references': fields.List(fields.Nested(reference_fields), required=False, description="The references pointing to the primary source"),
    'hierarchy':fields.List(fields.Nested(hierarchy_level_fields), required=False)
})

author_fields = Model('Author', {
    'id': fields.String(required=True, description='The local id (MongoID) of the author'),
    'name': fields.String(required=True, description='The author name', attribute="author_final_form"),
    'checked': fields.Boolean(required=True, description='Whether the author record has been manually checked'),
    'viaf_link': fields.String(required=False, description='The link to the author record in VIAF'),
})

book_fields = Model('Book', {
    'author':fields.List(fields.Nested(author_fields), required=False),
    'id': fields.String(required=True, description='The local ID (MongoID) of the book'),
    'bid': fields.String(required=False, description='The bibliographic ID (BID) of the book'),
    'title': fields.String(required=True, description='The books\'s title'),
    'year': fields.String(required=True, description='Publication year', attribute="publication_year"),
    'place': fields.String(required=True, description='Publication place', attribute="publication_place"),
    'country': fields.String(required=True, description='Publication country', attribute="publication_country"),
    'publisher': fields.String(required=True, description='The book\'s publisher'),
    'digitization_provenance': fields.String(required=True, description='The ID of the library that has digitized the book (if applicable)'),
    'incoming_references': fields.List(fields.Nested(reference_fields), required=False),
    'is_digitized': fields.Boolean(required=True, description='Whether a digitized version of the book is available'),
    'is_oa': fields.Boolean(required=True, description='Whether the digitized book is openly available (via the Scholar Library)'),
})

article_fields = Model('Article', {
    'id': fields.String(required=True, description='The local ID (MongoID) of the article'),
    'author':fields.List(fields.Nested(author_fields)),
    'bid': fields.String(required=False, attribute="journal_bid", description='Bibliographic ID (BID) of the containing journal issue'),
    'title': fields.String(required=True, description='The article\'s title'),
    'journal_short_title': fields.String(required=True, description='The journal\'s short title'),
    'year': fields.Integer(required=True, description='Publication year'),
    'volume': fields.String(required=True, description='Journal\'s volume'),
    'issue_number': fields.String(required=True, description='Journal\'s issue number'),
    'start_img_number': fields.String(required=True, description='Starting number of digitized page image'),
    'end_img_number': fields.String(required=True, description='Ending number of digitized page image'),
    'start_page_number': fields.String(required=True, description='Starting page number (physical pagination)'),
    'end_page_number': fields.String(required=True, description='Ending page number (physical pagination)'),
    'digitization_provenance': fields.String(required=True, description='The ID of the library that has digitized the book (if applicable)'),
    'incoming_references':fields.List(fields.Nested(reference_fields), required=False)
})

citing_publication_list = Model('CitingPublications', {
    'articles': fields.List(fields.Nested(article_fields)),
    'books' : fields.List(fields.Nested(book_fields)),
    })

cited_publication_list = Model.clone('CitedPublications', citing_publication_list, {
    'primary_sources' : fields.List(fields.Nested(primary_source_fields))
    })

keywords_with_tfidf = Model('KeywordsWithTfidf', {
    'keyword': fields.String(required = True),
    'tf': fields.Integer(required = True),
    'df': fields.Integer(required = True),
    'tfidf': fields.Float(required = True),
})

europeana_result = Model('EuropeanaResult', {
    'direct_url': fields.String(required = False),
    'europeana_url': fields.String(required = False),
    'provider': fields.String(required = False),
    'thumbnail': fields.String(required = False),
    'title': fields.String(required = False),
    'type': fields.String(required = False),
    'licence': fields.String(required = False),
    'year': fields.String(required = False),
    'lang': fields.String(required = False),
})

europeana_suggestions = Model('Suggestions', {
    'query': fields.String(
        required = True,
        description='The query sent to Europeana',
    ),
    'total': fields.Integer(
        required = True,
        description='Number of results found by Europeana',
    ),
    'cursor': fields.String(
        required = False,
        description='Cursor to the next page of results'
    ),
    'keywords': fields.List(fields.Nested(keywords_with_tfidf),
        required = False,
        description = 'List of keywords used to generate the query and their tf/df score.',
    ),
    'results': fields.List(fields.Nested(europeana_result, skip_none=True), #Do not fill None values with null
        required = True,
        description = 'A page of results from Europeana',
    ),
})
