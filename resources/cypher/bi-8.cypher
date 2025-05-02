MATCH
    (comment: Comment)-[a: hasTag]->(tag: Tag),
    (comment)-[b: hasCreator]->(person: Person),
    (person)-[c: hasInterest]->(tag),
    (person)-[d: knows]->(friend: Person)
WHERE
    comment.creationDate > 1289146646222 AND
    tag.name = 'Theodore_Roosevelt'
RETURN
    comment, tag, person, friend, a, b, c, d