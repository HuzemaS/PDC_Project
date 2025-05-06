#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <pthread.h>
#include <mpi.h>
#include <time.h>
#include <unistd.h>

// Define constants based on LiveJournal dataset
#define INF INT_MAX
#define MAX_NODES 4847571
#define MAX_EDGES 68993773
#define MAX_THREADS 16
#define MAX_BUCKETS 100000

// Edge structure
typedef struct {
    int src;
    int dest;
    int weight; // Default weight = 1 for LiveJournal
} Edge;

// Graph structure
typedef struct {
    int num_nodes;
    Edge *edges;
    int num_edges;
    int **adj;
    int *adj_sizes;
    int *node_to_partition;
} Graph;

// Edge Update structure
typedef enum {
    INSERT,
    DELETE
} UpdateType;

typedef struct {
    UpdateType type;
    int src;
    int dest;
    int weight;
} EdgeUpdate;

// Thread argument structure
typedef struct {
    Graph *graph;
    EdgeUpdate *updates;
    size_t start_idx;
    size_t end_idx;
    int *adjacency_changed;
    pthread_mutex_t *mutex;
} ThreadArg;

// Bucket thread argument
typedef struct {
    Graph *graph;
    int *dist;
    int *nodes;
    int num_nodes;
    int delta;
    int **next_buckets;
    int *bucket_sizes;
    int process_light;
    pthread_mutex_t *mutex;
} BucketThreadArg;

// Global mutex
pthread_mutex_t global_mutex;

// Function to initialize graph
void init_graph(Graph *graph, int num_nodes) {
    graph->num_nodes = num_nodes;
    graph->edges = (Edge *)malloc(MAX_EDGES * sizeof(Edge));
    graph->num_edges = 0;
    graph->adj = (int **)malloc(num_nodes * sizeof(int *));
    graph->adj_sizes = (int *)calloc(num_nodes, sizeof(int));
    graph->node_to_partition = (int *)malloc(num_nodes * sizeof(int));
    
    for (int i = 0; i < num_nodes; i++) {
        graph->adj[i] = (int *)malloc(50 * 2 * sizeof(int)); // Adjust based on avg degree
    }
}

// Function to free graph
void free_graph(Graph *graph) {
    for (int i = 0; i < graph->num_nodes; i++) {
        free(graph->adj[i]);
    }
    free(graph->adj);
    free(graph->adj_sizes);
    free(graph->edges);
    free(graph->node_to_partition);
}

// Compute adjacency list
void compute_adjacency_list(Graph *graph) {
    for (int i = 0; i < graph->num_nodes; i++) {
        graph->adj_sizes[i] = 0;
    }

    for (int i = 0; i < graph->num_edges; i++) {
        Edge e = graph->edges[i];
        int src = e.src;
        int dest = e.dest;
        int weight = e.weight;

        if (graph->adj_sizes[src] >= 50) {
            // Resize adjacency list if needed
            graph->adj[src] = (int *)realloc(graph->adj[src], (graph->adj_sizes[src] + 10) * 2 * sizeof(int));
        }

        graph->adj[src][graph->adj_sizes[src] * 2] = dest;
        graph->adj[src][graph->adj_sizes[src] * 2 + 1] = weight;
        graph->adj_sizes[src]++;
    }
}

// Identify boundary nodes
void identify_boundary_nodes(Graph *graph, int my_partition, int *boundary, int *boundary_size) {
    int *is_boundary = (int *)calloc(graph->num_nodes, sizeof(int));
    *boundary_size = 0;

    for (int node = 0; node < graph->num_nodes; node++) {
        if (graph->node_to_partition[node] != my_partition)
            continue;

        for (int i = 0; i < graph->adj_sizes[node]; i++) {
            int neighbor = graph->adj[node][i * 2];
            if (graph->node_to_partition[neighbor] != my_partition) {
                is_boundary[node] = 1;
                break;
            }
        }
    }

    for (int node = 0; node < graph->num_nodes; node++) {
        if (is_boundary[node]) {
            boundary[*boundary_size] = node;
            (*boundary_size)++;
        }
    }
    free(is_boundary);
}

// Load LiveJournal graph from file
void load_from_file(Graph *graph, const char *filename) {
    FILE *file = fopen(filename, "r");
    if (!file) {
        fprintf(stderr, "Error: Unable to open file %s\n", filename);
        exit(1);
    }

    printf("Loading graph from %s...\n", filename);
    fflush(stdout);

    // First pass: count edges and find max node ID
    int max_node = -1;
    int edge_count = 0;
    char line[256];
    
    while (fgets(line, sizeof(line), file)) {
        if (line[0] == '#') continue;
        
        int src, dest;
        if (sscanf(line, "%d %d", &src, &dest) == 2) {
            edge_count++;
            if (src > max_node) max_node = src;
            if (dest > max_node) max_node = dest;
            if (edge_count % 1000000 == 0) {
                printf("Reading edges: %dM\r", edge_count/1000000);
                fflush(stdout);
            }
        }
    }
    
    if (max_node == -1) {
        fprintf(stderr, "Error: No valid edges found in file\n");
        fclose(file);
        exit(1);
    }

    printf("\nFound %d edges, max node ID: %d\n", edge_count, max_node);
    
    // Initialize graph
    graph->num_nodes = max_node + 1;
    if (graph->num_nodes > MAX_NODES) {
        fprintf(stderr, "Error: Number of nodes (%d) exceeds MAX_NODES (%d)\n", 
                graph->num_nodes, MAX_NODES);
        exit(1);
    }
    
    init_graph(graph, graph->num_nodes);
    graph->num_edges = 0;

    // Second pass: read edges
    rewind(file);
    int edges_read = 0;
    while (fgets(line, sizeof(line), file)) {
        if (line[0] == '#') continue;
        
        int src, dest;
        if (sscanf(line, "%d %d", &src, &dest) == 2) {
            if (graph->num_edges >= MAX_EDGES) {
                fprintf(stderr, "Warning: Reached maximum edge count (%d)\n", MAX_EDGES);
                break;
            }
            if (src >= graph->num_nodes || dest >= graph->num_nodes) {
                fprintf(stderr, "Warning: Invalid node ID in edge %d->%d\n", src, dest);
                continue;
            }
            graph->edges[graph->num_edges].src = src;
            graph->edges[graph->num_edges].dest = dest;
            graph->edges[graph->num_edges].weight = 1; // Default weight
            graph->num_edges++;
            edges_read++;
            
            if (edges_read % 1000000 == 0) {
                printf("Loading edges: %dM/%dM\r", edges_read/1000000, edge_count/1000000);
                fflush(stdout);
            }
        }
    }

    fclose(file);
    printf("\nSuccessfully loaded %d edges\n", graph->num_edges);

    // Initialize node partitioning
    for (int i = 0; i < graph->num_nodes; i++) {
        graph->node_to_partition[i] = i % 4;
    }

    printf("Computing adjacency lists...\n");
    compute_adjacency_list(graph);
    printf("Graph initialization complete\n");
}

// Thread-safe update processing
void *process_updates_thread(void *arg) {
    ThreadArg *t_arg = (ThreadArg *)arg;
    Graph *graph = t_arg->graph;
    EdgeUpdate *updates = t_arg->updates;
    size_t start_idx = t_arg->start_idx;
    size_t end_idx = t_arg->end_idx;
    int *adjacency_changed = t_arg->adjacency_changed;
    pthread_mutex_t *mutex = t_arg->mutex;
    int local_changed = 0;

    for (size_t i = start_idx; i < end_idx; i++) {
        EdgeUpdate upd = updates[i];

        if (upd.src >= graph->num_nodes || upd.dest >= graph->num_nodes) {
            pthread_mutex_lock(mutex);
            fprintf(stderr, "Warning: Ignoring update with invalid node IDs: %d -> %d\n", upd.src, upd.dest);
            pthread_mutex_unlock(mutex);
            continue;
        }

        if (upd.type == INSERT) {
            int found = 0;
            pthread_mutex_lock(mutex);
            for (int j = 0; j < graph->adj_sizes[upd.src]; j++) {
                if (graph->adj[upd.src][j * 2] == upd.dest) {
                    graph->adj[upd.src][j * 2 + 1] = upd.weight;
                    found = 1;
                    break;
                }
            }
            pthread_mutex_unlock(mutex);

            if (!found) {
                pthread_mutex_lock(mutex);
                if (graph->num_edges >= MAX_EDGES) {
                    fprintf(stderr, "Warning: Cannot insert edge, reached maximum edge count\n");
                } else {
                    graph->edges[graph->num_edges].src = upd.src;
                    graph->edges[graph->num_edges].dest = upd.dest;
                    graph->edges[graph->num_edges].weight = upd.weight;
                    graph->num_edges++;
                    local_changed = 1;
                }
                pthread_mutex_unlock(mutex);
            }
        } else if (upd.type == DELETE) {
            pthread_mutex_lock(mutex);
            for (int j = 0; j < graph->num_edges; j++) {
                if (graph->edges[j].src == upd.src && graph->edges[j].dest == upd.dest) {
                    graph->edges[j] = graph->edges[graph->num_edges - 1];
                    graph->num_edges--;
                    local_changed = 1;
                    break;
                }
            }
            pthread_mutex_unlock(mutex);
        }
    }

    if (local_changed) {
        pthread_mutex_lock(mutex);
        *adjacency_changed = 1;
        pthread_mutex_unlock(mutex);
    }

    return NULL;
}

// Apply batch updates
void apply_updates(Graph *graph, EdgeUpdate *updates, int num_updates) {
    int adjacency_changed = 0;
    int num_threads = sysconf(_SC_NPROCESSORS_ONLN);
    num_threads = (num_threads > MAX_THREADS) ? MAX_THREADS : num_threads;
    num_threads = (num_threads < 1) ? 1 : num_threads;

    pthread_t threads[MAX_THREADS];
    ThreadArg thread_args[MAX_THREADS];
    size_t chunk_size = num_updates / num_threads;

    for (int i = 0; i < num_threads; i++) {
        thread_args[i].graph = graph;
        thread_args[i].updates = updates;
        thread_args[i].start_idx = i * chunk_size;
        thread_args[i].end_idx = (i == num_threads - 1) ? num_updates : (i + 1) * chunk_size;
        thread_args[i].adjacency_changed = &adjacency_changed;
        thread_args[i].mutex = &global_mutex;

        if (thread_args[i].start_idx < num_updates) {
            pthread_create(&threads[i], NULL, process_updates_thread, &thread_args[i]);
        }
    }

    for (int i = 0; i < num_threads; i++) {
        if (thread_args[i].start_idx < num_updates) {
            pthread_join(threads[i], NULL);
        }
    }

    if (adjacency_changed) {
        compute_adjacency_list(graph);
    }
}

// Compute optimal delta
int compute_optimal_delta(Graph *graph) {
    if (graph->num_edges == 0) return 10;

    // For LiveJournal with uniform weight=1, we can use a fixed delta
    return 10; // Optimal for unweighted or uniformly weighted graphs
}

// Identify affected nodes
void identify_affected_nodes(Graph *graph, EdgeUpdate *updates, int num_updates, int *dist, int *affected, int *affected_size) {
    int *is_affected = (int *)calloc(graph->num_nodes, sizeof(int));
    *affected_size = 0;

    for (int i = 0; i < num_updates; i++) {
        EdgeUpdate upd = updates[i];
        is_affected[upd.src] = 1;
        is_affected[upd.dest] = 1;

        if (upd.type == INSERT) {
            int potential_new_dist = dist[upd.src] + upd.weight;
            if (potential_new_dist < dist[upd.dest]) {
                is_affected[upd.dest] = 1;
            }
        } else if (upd.type == DELETE) {
            for (int j = 0; j < graph->adj_sizes[upd.src]; j++) {
                is_affected[graph->adj[upd.src][j * 2]] = 1;
            }
            for (int j = 0; j < graph->adj_sizes[upd.dest]; j++) {
                is_affected[graph->adj[upd.dest][j * 2]] = 1;
            }
        }
    }

    for (int i = 0; i < graph->num_nodes; i++) {
        if (is_affected[i]) {
            affected[*affected_size] = i;
            (*affected_size)++;
        }
    }
    free(is_affected);
}

// Thread worker for delta stepping
void *process_bucket_nodes(void *arg) {
    BucketThreadArg *t_arg = (BucketThreadArg *)arg;
    Graph *graph = t_arg->graph;
    int *dist = t_arg->dist;
    int *nodes = t_arg->nodes;
    int num_nodes = t_arg->num_nodes;
    int delta = t_arg->delta;
    int **next_buckets = t_arg->next_buckets;
    int *bucket_sizes = t_arg->bucket_sizes;
    int process_light = t_arg->process_light;
    pthread_mutex_t *mutex = t_arg->mutex;

    int *local_relaxed = (int *)malloc(graph->num_nodes * sizeof(int));
    int *local_relaxed_nodes = (int *)malloc(graph->num_nodes * sizeof(int));
    int local_relaxed_count = 0;

    for (int i = 0; i < graph->num_nodes; i++) {
        local_relaxed[i] = INF;
    }

    for (int i = 0; i < num_nodes; i++) {
        int u = nodes[i];
        if (u >= graph->num_nodes) continue;

        for (int j = 0; j < graph->adj_sizes[u]; j++) {
            int v = graph->adj[u][j * 2];
            int weight = graph->adj[u][j * 2 + 1];

            if ((process_light && weight <= delta) || (!process_light && weight > delta)) {
                int new_dist = dist[u] + weight;
                if (new_dist < dist[v] && new_dist < local_relaxed[v]) {
                    local_relaxed[v] = new_dist;
                    local_relaxed_nodes[local_relaxed_count++] = v;
                }
            }
        }
    }

    pthread_mutex_lock(mutex);
    for (int i = 0; i < local_relaxed_count; i++) {
        int v = local_relaxed_nodes[i];
        int new_dist = local_relaxed[v];
        if (new_dist < dist[v]) {
            dist[v] = new_dist;
            int new_bucket = new_dist / delta;
            if (new_bucket >= MAX_BUCKETS) continue; // Skip if beyond our bucket limit
            next_buckets[new_bucket][bucket_sizes[new_bucket]++] = v;
        }
    }
    pthread_mutex_unlock(mutex);

    free(local_relaxed);
    free(local_relaxed_nodes);
    return NULL;
}

// Delta-Stepping SSSP
void delta_stepping_sssp(Graph *graph, int *dist, int source, int delta, int *affected, int affected_size) {
    int num_threads = sysconf(_SC_NPROCESSORS_ONLN);
    num_threads = (num_threads > MAX_THREADS) ? MAX_THREADS : num_threads;
    num_threads = (num_threads < 1) ? 1 : num_threads;

    // Initialize buckets
    int **buckets = (int **)malloc(MAX_BUCKETS * sizeof(int *));
    int *bucket_sizes = (int *)calloc(MAX_BUCKETS, sizeof(int));
    for (int i = 0; i < MAX_BUCKETS; i++) {
        buckets[i] = (int *)malloc(MAX_NODES * sizeof(int));
        bucket_sizes[i] = 0;
    }

    // Initialize distances
    if (affected_size == 0) {
        for (int i = 0; i < graph->num_nodes; i++) {
            dist[i] = INF;
        }
        dist[source] = 0;
        buckets[0][bucket_sizes[0]++] = source;
    } else {
        for (int i = 0; i < affected_size; i++) {
            int node = affected[i];
            if (dist[node] != INF) {
                int bucket_idx = dist[node] / delta;
                if (bucket_idx >= MAX_BUCKETS) continue;
                buckets[bucket_idx][bucket_sizes[bucket_idx]++] = node;
            }
        }
    }

    // Process buckets
    for (int current_idx = 0; current_idx < MAX_BUCKETS; current_idx++) {
        if (bucket_sizes[current_idx] == 0) continue;

        int *current_nodes = buckets[current_idx];
        int num_current_nodes = bucket_sizes[current_idx];
        bucket_sizes[current_idx] = 0;

        // Process light edges
        int **next_light_buckets = (int **)malloc(MAX_BUCKETS * sizeof(int *));
        int *light_bucket_sizes = (int *)calloc(MAX_BUCKETS, sizeof(int));
        for (int i = 0; i < MAX_BUCKETS; i++) {
            next_light_buckets[i] = (int *)malloc(MAX_NODES * sizeof(int));
            light_bucket_sizes[i] = 0;
        }

        if (num_current_nodes > num_threads) {
            pthread_t threads[MAX_THREADS];
            BucketThreadArg thread_args[MAX_THREADS];
            int chunk_size = num_current_nodes / num_threads;

            for (int t = 0; t < num_threads; t++) {
                thread_args[t].graph = graph;
                thread_args[t].dist = dist;
                thread_args[t].nodes = current_nodes + (t * chunk_size);
                thread_args[t].num_nodes = (t == num_threads - 1) ? (num_current_nodes - t * chunk_size) : chunk_size;
                thread_args[t].delta = delta;
                thread_args[t].next_buckets = next_light_buckets;
                thread_args[t].bucket_sizes = light_bucket_sizes;
                thread_args[t].process_light = 1;
                thread_args[t].mutex = &global_mutex;

                if (thread_args[t].num_nodes > 0) {
                    pthread_create(&threads[t], NULL, process_bucket_nodes, &thread_args[t]);
                }
            }

            for (int t = 0; t < num_threads; t++) {
                if (thread_args[t].num_nodes > 0) {
                    pthread_join(threads[t], NULL);
                }
            }
        } else {
            BucketThreadArg arg = {graph, dist, current_nodes, num_current_nodes, delta, 
                                 next_light_buckets, light_bucket_sizes, 1, &global_mutex};
            process_bucket_nodes(&arg);
        }

        // Move light bucket nodes
        for (int i = 0; i < MAX_BUCKETS; i++) {
            for (int j = 0; j < light_bucket_sizes[i]; j++) {
                if (i >= MAX_BUCKETS) continue;
                buckets[i][bucket_sizes[i]++] = next_light_buckets[i][j];
            }
        }

        // Process heavy edges
        int **next_heavy_buckets = (int **)malloc(MAX_BUCKETS * sizeof(int *));
        int *heavy_bucket_sizes = (int *)calloc(MAX_BUCKETS, sizeof(int));
        for (int i = 0; i < MAX_BUCKETS; i++) {
            next_heavy_buckets[i] = (int *)malloc(MAX_NODES * sizeof(int));
            heavy_bucket_sizes[i] = 0;
        }

        if (num_current_nodes > num_threads) {
            pthread_t threads[MAX_THREADS];
            BucketThreadArg thread_args[MAX_THREADS];
            int chunk_size = num_current_nodes / num_threads;

            for (int t = 0; t < num_threads; t++) {
                thread_args[t].graph = graph;
                thread_args[t].dist = dist;
                thread_args[t].nodes = current_nodes + (t * chunk_size);
                thread_args[t].num_nodes = (t == num_threads - 1) ? (num_current_nodes - t * chunk_size) : chunk_size;
                thread_args[t].delta = delta;
                thread_args[t].next_buckets = next_heavy_buckets;
                thread_args[t].bucket_sizes = heavy_bucket_sizes;
                thread_args[t].process_light = 0;
                thread_args[t].mutex = &global_mutex;

                if (thread_args[t].num_nodes > 0) {
                    pthread_create(&threads[t], NULL, process_bucket_nodes, &thread_args[t]);
                }
            }

            for (int t = 0; t < num_threads; t++) {
                if (thread_args[t].num_nodes > 0) {
                    pthread_join(threads[t], NULL);
                }
            }
        } else {
            BucketThreadArg arg = {graph, dist, current_nodes, num_current_nodes, delta, 
                                 next_heavy_buckets, heavy_bucket_sizes, 0, &global_mutex};
            process_bucket_nodes(&arg);
        }

        // Move heavy bucket nodes
        for (int i = 0; i < MAX_BUCKETS; i++) {
            for (int j = 0; j < heavy_bucket_sizes[i]; j++) {
                if (i >= MAX_BUCKETS) continue;
                buckets[i][bucket_sizes[i]++] = next_heavy_buckets[i][j];
            }
        }

        // Free temporary buckets
        for (int i = 0; i < MAX_BUCKETS; i++) {
            free(next_light_buckets[i]);
            free(next_heavy_buckets[i]);
        }
        free(next_light_buckets);
        free(next_heavy_buckets);
        free(light_bucket_sizes);
        free(heavy_bucket_sizes);
    }

    // Free buckets
    for (int i = 0; i < MAX_BUCKETS; i++) {
        free(buckets[i]);
    }
    free(buckets);
    free(bucket_sizes);
}

// Synchronize boundary nodes
void sync_boundary_nodes(Graph *graph, int my_rank, int num_processes, int *boundary, int boundary_size, int *distances) {
    for (int r = 0; r < num_processes; r++) {
        if (r == my_rank) continue;

        int *send_data = (int *)malloc(boundary_size * 2 * sizeof(int));
        for (int i = 0; i < boundary_size; i++) {
            send_data[i * 2] = boundary[i];
            send_data[i * 2 + 1] = distances[boundary[i]];
        }

        int send_size = boundary_size;
        int recv_size;

        MPI_Sendrecv(&send_size, 1, MPI_INT, r, 0, 
                    &recv_size, 1, MPI_INT, r, 0, 
                    MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        int *recv_buffer = (int *)malloc(recv_size * 2 * sizeof(int));

        MPI_Sendrecv(send_data, send_size * 2, MPI_INT, r, 1,
                    recv_buffer, recv_size * 2, MPI_INT, r, 1,
                    MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        for (int i = 0; i < recv_size; i++) {
            int node_id = recv_buffer[i * 2];
            int dist_value = recv_buffer[i * 2 + 1];
            if (node_id < graph->num_nodes && dist_value < distances[node_id]) {
                distances[node_id] = dist_value;
            }
        }

        free(send_data);
        free(recv_buffer);
    }
}

// Check if repartitioning is needed
int should_repartition(Graph *graph, EdgeUpdate *updates, int num_updates, int update_threshold, float imbalance_threshold) {
    if (num_updates > update_threshold) return 1;

    int *partition_sizes = (int *)calloc(4, sizeof(int)); // Assuming 4 partitions
    int max_size = 0, min_size = graph->num_nodes, total_nodes = 0;

    for (int i = 0; i < graph->num_nodes; i++) {
        int p = graph->node_to_partition[i];
        partition_sizes[p]++;
    }

    for (int i = 0; i < 4; i++) {
        if (partition_sizes[i] > max_size) max_size = partition_sizes[i];
        if (partition_sizes[i] < min_size) min_size = partition_sizes[i];
        total_nodes += partition_sizes[i];
    }

    float avg_size = total_nodes / 4.0f;
    float imbalance = max_size / avg_size;

    free(partition_sizes);
    return imbalance > imbalance_threshold;
}

int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);
    pthread_mutex_init(&global_mutex, NULL);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    char *graph_file = "soc-LiveJournal1.txt";
    int source_node = 0;
    int num_updates = 100;

    if (argc > 1) graph_file = argv[1];
    if (argc > 2) source_node = atoi(argv[2]);
    if (argc > 3) num_updates = atoi(argv[3]);

    if (rank == 0) {
        printf("LiveJournal SSSP with Delta-Stepping Algorithm\n");
        printf("Graph file: %s\n", graph_file);
        printf("Source node: %d\n", source_node);
        printf("Number of updates: %d\n", num_updates);
        printf("MPI processes: %d\n", size);
    }

    Graph graph;
    if (rank == 0) {
        load_from_file(&graph, graph_file);
    }

    // Broadcast graph structure
    MPI_Bcast(&graph.num_nodes, 1, MPI_INT, 0, MPI_COMM_WORLD);
    MPI_Bcast(&graph.num_edges, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank != 0) {
        init_graph(&graph, graph.num_nodes);
    }

    // Broadcast edges
    int *edge_data = NULL;
    if (rank == 0) {
        edge_data = (int *)malloc(graph.num_edges * 3 * sizeof(int));
        for (int i = 0; i < graph.num_edges; i++) {
            edge_data[i * 3] = graph.edges[i].src;
            edge_data[i * 3 + 1] = graph.edges[i].dest;
            edge_data[i * 3 + 2] = graph.edges[i].weight;
        }
    }

    MPI_Bcast(edge_data, graph.num_edges * 3, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank != 0) {
        for (int i = 0; i < graph.num_edges; i++) {
            graph.edges[i].src = edge_data[i * 3];
            graph.edges[i].dest = edge_data[i * 3 + 1];
            graph.edges[i].weight = edge_data[i * 3 + 2];
        }
    }

    // Broadcast node partitions
    MPI_Bcast(graph.node_to_partition, graph.num_nodes, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        free(edge_data);
    }

    compute_adjacency_list(&graph);

    // Identify boundary nodes
    int *boundary = (int *)malloc(graph.num_nodes * sizeof(int));
    int boundary_size;
    identify_boundary_nodes(&graph, rank, boundary, &boundary_size);

    if (rank == 0) {
        printf("Computing initial SSSP...\n");
    }

    double start_time = MPI_Wtime();

    int *distances = (int *)malloc(graph.num_nodes * sizeof(int));
    for (int i = 0; i < graph.num_nodes; i++) {
        distances[i] = INF;
    }

    int delta = compute_optimal_delta(&graph);
    if (rank == 0) {
        printf("Using delta value: %d\n", delta);
    }

    if (graph.node_to_partition[source_node] == rank || rank == 0) {
        delta_stepping_sssp(&graph, distances, source_node, delta, NULL, 0);
    }

    sync_boundary_nodes(&graph, rank, size, boundary, boundary_size, distances);

    double initial_sssp_time = MPI_Wtime() - start_time;

    if (rank == 0) {
        printf("Initial SSSP completed in %.2f seconds\n", initial_sssp_time);
        printf("Generating %d random updates...\n", num_updates);
    }

    // Generate random updates
    EdgeUpdate *updates = (EdgeUpdate *)malloc(num_updates * sizeof(EdgeUpdate));
    int num_updates_actual = 0;
    if (rank == 0) {
        srand(42);
        for (int i = 0; i < num_updates; i++) {
            updates[i].type = (i % 2 == 0) ? INSERT : DELETE;
            updates[i].src = rand() % graph.num_nodes;
            updates[i].dest = rand() % graph.num_nodes;
            updates[i].weight = 1; // LiveJournal edges have weight=1
        }
        num_updates_actual = num_updates;
    }

    MPI_Bcast(&num_updates_actual, 1, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank != 0) {
        updates = (EdgeUpdate *)realloc(updates, num_updates_actual * sizeof(EdgeUpdate));
    }

    int *update_data = (int *)malloc(num_updates_actual * 4 * sizeof(int));
    if (rank == 0) {
        for (int i = 0; i < num_updates_actual; i++) {
            update_data[i * 4] = updates[i].type;
            update_data[i * 4 + 1] = updates[i].src;
            update_data[i * 4 + 2] = updates[i].dest;
            update_data[i * 4 + 3] = updates[i].weight;
        }
    }

    MPI_Bcast(update_data, num_updates_actual * 4, MPI_INT, 0, MPI_COMM_WORLD);

    if (rank != 0) {
        for (int i = 0; i < num_updates_actual; i++) {
            updates[i].type = update_data[i * 4];
            updates[i].src = update_data[i * 4 + 1];
            updates[i].dest = update_data[i * 4 + 2];
            updates[i].weight = update_data[i * 4 + 3];
        }
    }
    free(update_data);

    int need_repartition = should_repartition(&graph, updates, num_updates_actual, 200, 1.2);
    if (need_repartition && rank == 0) {
        printf("Repartitioning graph due to load imbalance...\n");
        // Simple repartitioning - in practice you'd want something more sophisticated
        for (int i = 0; i < graph.num_nodes; i++) {
            graph.node_to_partition[i] = i % size; // Now partition across all MPI processes
        }
    }

    // Broadcast new partitions if repartitioned
    MPI_Bcast(graph.node_to_partition, graph.num_nodes, MPI_INT, 0, MPI_COMM_WORLD);

    start_time = MPI_Wtime();

    apply_updates(&graph, updates, num_updates_actual);

    int *affected = (int *)malloc(graph.num_nodes * sizeof(int));
    int affected_size;
    identify_affected_nodes(&graph, updates, num_updates_actual, distances, affected, &affected_size);

    if (rank == 0) {
        printf("Identified %d affected nodes\n", affected_size);
    }

    delta_stepping_sssp(&graph, distances, source_node, delta, affected, affected_size);

    sync_boundary_nodes(&graph, rank, size, boundary, boundary_size, distances);

    double update_sssp_time = MPI_Wtime() - start_time;

    if (rank == 0) {
        printf("Incremental SSSP completed in %.2f seconds\n", update_sssp_time);
        printf("Speedup over full recomputation: %.2fx\n", initial_sssp_time / update_sssp_time);
        
        printf("Sample distances from source node %d:\n", source_node);
        for (int i = 0; i < 10 && i < graph.num_nodes; i++) {
            if (distances[i] == INF) {
                printf("Node %d: INF\n", i);
            } else {
                printf("Node %d: %d\n", i, distances[i]);
            }
        }
    }

    // Cleanup
    free(boundary);
    free(distances);
    free(updates);
    free(affected);
    free_graph(&graph);
    pthread_mutex_destroy(&global_mutex);
    MPI_Finalize();
    return 0;
}