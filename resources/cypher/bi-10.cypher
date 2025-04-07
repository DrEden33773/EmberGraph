MATCH
    (post: Post)-[a: hasTag]->(tag: Tag),
    (tag)-[b: hasType]->(tagClass: Tagclass),
    (post)-[c: hasTag]->(otherTag: Tag),
    (post)-[d: hasCreator]->(expertCandidatePerson: Person),
    (startPerson: Person)-[e: knows]->(expertCandidatePerson),
    (expertCandidatePerson)-[f: isLocatedIn]->(city: City),
    (city)-[g: isPartOf]->(country: Country)
WHERE
    startPerson.id = 6 AND
    tagClass.name = 'Comedian'
RETURN
    startPerson,
    tagClass,
    post,
    tag,
    otherTag,
    expertCandidatePerson,
    city,
    country,
    a,
    b,
    c,
    d,
    e,
    f,
    g