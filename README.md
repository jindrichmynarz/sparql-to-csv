# sparql-to-csv

A command-line tool to stream SPARQL results to CSV. The tool is primarily intended to support data preparation for analysis that requires tabular input. It helps you avoid writing ad hoc scripts to piece larger tabular datasets out of results of many SPARQL queries. It allows to generate queries from [Mustache](https://mustache.github.io) templates, either to execute paged queries or to execute queries based on results of other queries. 

## Usage

Use a [released executable](https://github.com/jindrichmynarz/sparql-to-csv/releases) or compile using [Leiningen](http://leiningen.org):

```sh
git clone https://github.com/jindrichmynarz/sparql-to-csv.git
cd sparql-to-csv
lein bin
```

Then you can run the created executable file to find out about the configuration options:
 
```sh
target/sparql_to_csv --help
```

Example of use:

```sh
target/sparql_to_csv --endpoint http://localhost:8890/sparql \
                     --page-size 1000 \
                     paged_query.mustache > results.csv
```

There are two main use cases for this tool: paged queries and piped queries.

### Paged queries

The first one is paged execution of SPARQL `SELECT` queries. RDF stores often limit the number of rows a SPARQL `SELECT` query can retrieve in one go and thus avoid the load such queries impose on the store. For queries that select more results than the limit per one request their execution must be split into several requests if complete results need to be obtained. One way to partition such queries is to split them into pages delimited by `LIMIT` and `OFFSET`, indicating the size and the start index, respectively, of a page. Paging requires the results to have a deterministic order, which can be achieved by using an `ORDER BY` clause. Due to limitations of some RDF stores (see [Virtuoso's documentation on this topic](https://virtuoso.openlinksw.com/dataspace/doc/dav/wiki/Main/VirtTipsAndTricksHowToHandleBandwidthLimitExceed)), the paged queries may need to contain an inner sub-`SELECT` that with an `ORDER BY` clause wrapped by an outer `SELECT` that slices a page from the ordered results using `LIMIT` and `OFFSET`, like this:

```sparql
PREFIX dbo: <http://dbpedia.org/ontology/>

SELECT ?person 
WHERE {
  {
    SELECT DISTINCT ?person
    WHERE {
      ?person a dbo:Person .
    }
    ORDER BY ?person
  }
}
LIMIT 10000
OFFSET 40000
```

In order to run paged queries you need to provide the tool with a Mustache template to generate the queries for the individual pages. These queries must contain a `{{limit}}` and `{{offset}}` parameters, like so:

```sparql
PREFIX dbo: <http://dbpedia.org/ontology/>

SELECT ?person 
WHERE {
  {
    SELECT DISTINCT ?person
    WHERE {
      ?person a dbo:Person .
    }
    ORDER BY ?person
  }
}
LIMIT {{limit}}
OFFSET {{offset}}
```

The `limit` is set by the `--page-size` parameter. The offset is incremented by the page size in each successive request. The execution of paged queries stops when an individual query returns empty results.

### Piped queries 

It may be desirable to decompose complex queries into several simpler queries to avoid limit on demanding queries due to performance. For example, for each person in a dataset we may want to retrieve its complex description. While this may be possible to achieve by using a sub-`SELECT` to page through the individual persons and an outer `SELECT` to compose their descriptions, such query would be more demanding since it both sorts the persons and selects their descriptions. Consequently, it may not be possible to run such query since it would end with a time-out. Instead, this query can be decomposed into two queries. The first one selects persons in the paged manner described above, while the second one receives results of the first query one by one and fetches their descriptions.

This approach is also useful when you need to query one SPARQL endpoint using data from another SPARQL endpoint. While this is feasible using [federated queries](https://www.w3.org/TR/sparql11-federated-query), they too suffer from performance problems.

Piped queries take CSV input and for each line they execute a query generated from a Mustache template that is provided with the line's data as parameters. For example, the CSV generated by the query above contains a column `person`, which can be used in a query template as `{{person}}`:

```sparql
PREFIX dbo: <http://dbpedia.org/ontology/>
PREFIX dbp: <http://dbpedia.org/property/>

SELECT (<{{person}}> AS ?person) ?name ?birthDate ?deathDate
WHERE {
  <{{person}}> dbp:name ?name ;
    dbo:birthDate ?birthDate .
  OPTIONAL {
    <{{person}}> dbo:deathDate ?deathDate .
  }
}
```

The input CSV must have a header with column names. In order to be usable in Mustache template, the column names in the input CSV can contain only ASCII characters, `?`, `!`, `/`, `.`, or `-`. For example, `right!` is allowed, while `mélangé` is not.

Piped queries enable to create data processing pipelines. For instance, if the first query is stored in the `persons.mustache` file and the second query is stored as `describe_person.mustache`, then we can run them in pipeline using the following command:

```sh
sparql_to_csv -e http://dbpedia.org/sparql persons.mustache |
  sparql_to_csv -e http://dbpedia.org/sparql describe_person.mustache
```

By default the piped input is replaced by the output query results. However, using the `--extend` parameter extends the input with the results. Each result row is append to its input row. This allows you to combine data from multiple queries. Piped queries can be arbitrarily chained and allow joining data across many SPARQL endpoints.

## License

Copyright © 2016 Jindřich Mynarz

Distributed under the Eclipse Public License version 1.0.
