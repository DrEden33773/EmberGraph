MATCH
    (comment: Comment)-[a: replyOf]->(post: Post),
    (comment)-[b: hasCreator]->(person: Person)
WHERE
    comment.creationDate > 1290666911352 AND
    post.language = 'tk'
RETURN
    comment, post, person, a, b