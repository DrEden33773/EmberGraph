MATCH
    (person1: Person)-[a: knows]->(person2: Person),
    (person1)-[b: workAt]->(company: Company),
    (person1)-[c: studyAt]->(university: University),
    (person2)-[d: studyAt]->(university)
WHERE
    person2.id = 8796093022379
    AND person1.id <> 8796093022379
    AND company.name = 'MDLR_Airlines'
RETURN
    person1,
    person2,
    company,
    university,
    a,
    b,
    c,
    d