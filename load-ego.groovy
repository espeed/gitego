g = TitanFactory.open('db/titan-gitego')

g.makeKey('login').dataType(String.class).indexed(Vertex.class).unique().make()
g.makeKey('full_name').dataType(String.class).indexed(Vertex.class).unique().make()
g.makeLabel('starred').make()
g.commit()

delim = "::"

userFile = 'data/transformed/users.dat'
repoFile = 'data/transformed/repos.dat'
starredFile = 'data/transformed/starred.dat'

userKeys = ["id", "login", "avatar_url", "gravatar_id", "html_url", "type"]
repoKeys = ["id",
            "name",
            "full_name",
            "owner",
            "fork",
            "created_at",
            "updated_at",
            "pushed_at",
            "homepage",
            "size",
            "watchers_count",
            "language",
            "forks_count",
            "forks",
            "watchers"]


addVertices = { dataFile, keys -> 

  data = new File(dataFile)
  
  counter = 0
  data.eachLine { 

    properties = [:]   // property map
    
    vals = it.split(delim)

    pairs = [keys, vals].transpose()
    pairs.each{ k, v -> properties[k] = v }
    
    id = properties["id"]
    properties.remove("id")   // id is rerved for vertex id

    v = g.addVertex(id, properties)
    //println v.map()
    
    counter += 1
    if (counter % 1000 == 0) { 
      println "Commit..." + counter
      g.commit()
    }

  }
  
  println "Commit..." + counter
  g.commit()
  
}


addEdges = { dataFile ->

  data = new File(dataFile)

  counter = 0
  data.eachLine { 

    (userName, label, repoName) = it.split(delim)
    
    user = g.V('login', userName)
    repo = g.V('full_name', repoName)

    user = user.hasNext() ? user.next() : null
    repo = repo.hasNext() ? repo.next() : null

    (user != null && repo !=null ) ? g.addEdge(null, user, repo, label) : null


    counter += 1
    if (counter % 1000 == 0) { 
      println "Commit..." + counter
      g.commit()
    }
        
  }

  println "Commit..." + counter
  g.commit()
}

addVertices(userFile, userKeys)
addVertices(repoFile, repoKeys)
addEdges(starredFile)
