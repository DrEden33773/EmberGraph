PROFILE
MATCH
    (comment: Comment)-[a: replyOf]->(post: Post),
    (post)-[b: hasCreator]->(person: Person)
WHERE
    comment.creationDate >= 1290666911352 AND
    post.creationDate >= 1290661462488
RETURN
    comment, post, person, a, b