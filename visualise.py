import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

# Step 1: Load your dataset (replace this with the actual data source, e.g., Cassandra query or CSV)
# Example data
data = {
    "search_term": [
        "bill clinton", "mission impossible film series", "brics",
        "bill clinton", "katie taylor", "brics"
    ],
    "source": [
        "other-search", "the penguin tv series", "other-search",
        "election", "other-search", "other-search"
    ],
    "type": ["external", "other", "external", "other", "external", "external"],
    "count": [305804, 18, 304875, 200, 491562, 120]
}

# Load into a DataFrame
df = pd.DataFrame(data)

# Step 2: Filter for a specific search term
search_term = "bill clinton"  # You can dynamically input this
filtered_data = df[df["search_term"] == search_term]

# Step 3: Create a directed graph
G = nx.DiGraph()

# Update edge labels to include type
for _, row in filtered_data.iterrows():
    G.add_edge(row["source"], search_term, weight=row["count"], type=row["type"])

# Draw the graph
plt.figure(figsize=(10, 8))
pos = nx.spring_layout(G)
nx.draw(
    G,
    pos,
    with_labels=True,
    node_size=3000,
    node_color="lightblue",
    font_size=10,
    font_weight="bold",
    arrowsize=20,
)
labels = {(u, v): f"{d['weight']} ({d['type']})" for u, v, d in G.edges(data=True)}
nx.draw_networkx_edge_labels(G, pos, edge_labels=labels)

plt.title(f"Traffic Sources for '{search_term}'", fontsize=14)
plt.show()


# import pandas as pd
# import networkx as nx
# import matplotlib.pyplot as plt

# # Step 1: Load the dataset
# # Replace this with your actual data source (e.g., Cassandra query or a file)
# data = {
#     "search_term": [
#         "bill clinton", "bill clinton", "mission impossible film series",
#         "brics", "katie taylor", "bill clinton", "hillary clinton", "clinton foundation"
#     ],
#     "source": [
#         "other-search", "the penguin tv series", "the penguin tv series",
#         "other-search", "other-search", "hillary clinton", "bill clinton", "bill clinton"
#     ],
#     "type": ["external", "other", "other", "external", "external", "link", "link", "link"],
#     "count": [305804, 200, 18, 304875, 491562, 120, 180, 150]
# }

# # Load data into a DataFrame
# df = pd.DataFrame(data)

# # Step 2: Define the target search term
# target_search_term = "bill clinton"

# # Step 3: Filter for incoming and outgoing edges
# incoming_edges = df[df["search_term"] == target_search_term]
# outgoing_edges = df[df["source"] == target_search_term]

# # Step 4: Create a directed graph
# G = nx.DiGraph()

# # Add incoming edges (source -> target_search_term)
# for _, row in incoming_edges.iterrows():
#     G.add_edge(row["source"], row["search_term"], weight=row["count"])

# # Add outgoing edges (target_search_term -> destination)
# for _, row in outgoing_edges.iterrows():
#     G.add_edge(row["search_term"], row["source"], weight=row["count"])

# # Step 5: Visualize the graph
# plt.figure(figsize=(12, 10))  # Adjust figure size for readability
# pos = nx.spring_layout(G)  # Layout for positioning nodes

# # Draw the graph
# nx.draw(
#     G,
#     pos,
#     with_labels=True,
#     node_size=3000,
#     node_color="lightblue",
#     font_size=10,
#     font_weight="bold",
#     arrowsize=20
# )

# # Add edge labels (weights)
# edge_labels = {(u, v): f"{d['weight']}" for u, v, d in G.edges(data=True)}
# nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)

# # Add title
# plt.title(f"Incoming and Outgoing Traffic for '{target_search_term}'", fontsize=14)
# plt.show()
