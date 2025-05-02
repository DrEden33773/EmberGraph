MATCH
    (country: Country)<-[a: isPartOf]-(city: City),
    (city)<-[b: isLocatedIn]-(person: Person),
    (person)-[c: hasModerator]-(forum: Forum),
    (forum)-[d: containerOf]->(post: Post),
    (post)<-[e: replyOf]-(comment: Comment),
    (comment)-[f: hasTag]->(tag: Tag),
    (tag)-[g: hasType]-(tagClass: Tagclass)
WHERE
    country.name = 'Afghanistan' AND
    tagClass.name = 'Album'
RETURN country, city, person, forum, post, comment, tag, tagClass, a, b, c, d, e, f, g
ORDER BY country.name, tagClass.name