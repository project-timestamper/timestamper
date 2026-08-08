// get the top 1000 most popular repositories on github

const QUERY = `
  query ReposByStars($query: String!, $cursor: String) {
    search(
      query: $query,
      type: REPOSITORY,
      first: 100,
      after: $cursor
    ) {
      pageInfo {
        hasNextPage
        endCursor
      }
      nodes {
        ... on Repository {
          id
          name
          owner { login }
          stargazerCount
          defaultBranchRef {
            target {
              ... on Commit {
                oid
              }
            }
          }
        }
      }
    }
  }
`

const GITHUB_TOKEN = process.env.GITHUB_TOKEN
if (!GITHUB_TOKEN) throw new Error('Set GITHUB_TOKEN')

const ENDPOINT = 'https://api.github.com/graphql'
const PER_PAGE = 100
const BATCH_LIMIT = 1000

async function graphql (query, variables) {
  const res = await fetch(ENDPOINT, {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${GITHUB_TOKEN}`,
      'Content-Type': 'application/json'
    },
    body: JSON.stringify({ query, variables })
  })

  if (!res.ok) {
    throw new Error(await res.text())
  }
  return res.json()
}

async function fetchBatch (starUpperBound) {
  let cursor = null
  const collected = []
  let lastStar = null

  while (collected.length < BATCH_LIMIT) {
    const starFilter = starUpperBound === null
      ? 'stars:>0'
      : `stars:<${starUpperBound}`

    const searchQuery = `${starFilter} sort:stars-desc`

    const data = await graphql(QUERY, {
      query: searchQuery,
      cursor
    })

    const search = data.data.search

    for (const repo of search.nodes) {
      if (!repo.defaultBranchRef) continue

      collected.push(repo)
      lastStar = repo.stargazerCount

      if (collected.length === BATCH_LIMIT) break
    }

    if (!search.pageInfo.hasNextPage) break
    cursor = search.pageInfo.endCursor
  }

  return { repos: collected, lastStar }
}

async function run (totalTarget = 2000) {
  const all = []
  let starUpperBound = null

  while (all.length < totalTarget) {
    const { repos, lastStar } = await fetchBatch(starUpperBound)

    if (repos.length === 0) break

    all.push(...repos)
    starUpperBound = lastStar

    console.log(
      `Collected ${all.length}, next stars < ${starUpperBound}`
    )
  }

  // Final global sort (safety)
  all.sort((a, b) =>
    b.stargazerCount - a.stargazerCount ||
    a.id.localeCompare(b.id)
  )

  // Emit results
  for (const r of all.slice(0, totalTarget)) {
    console.log(
      `${r.owner.login}/${r.name} ${r.stargazerCount} ${r.defaultBranchRef.target.oid}`
    )
  }
}

run(3000).catch(console.error)
