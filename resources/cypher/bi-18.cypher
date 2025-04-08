PROFILE
MATCH
    (person: Person)-[a: knows]->(person1: Person),
    (person)-[b: knows]->(person2: Person),
    (person1)-[c: hasInterest]->(tag: Tag),
    (person2)-[d: hasInterest]->(tag)
WHERE
    person1.id = 143
    AND person2.id <> 143
    AND tag.name = 'Elizabeth_I_of_England'
RETURN
    person,
    person1,
    person2,
    tag,
    a,
    b,
    c,
    d