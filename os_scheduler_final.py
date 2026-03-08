import sys
import re

# ==========================================
#  OS SCHEDULER PROJECT - FINAL VERSION
#  Algorithm: Priority Scheduling + Round Robin + Aging + Deadlock Detection
#  Features: Preemptive scheduling, Gantt Chart, Deadlock Recovery, Aging
# ==========================================

class Process:
    """
    Represents a single process in the system.
    Each process has:
    - Unique identifier (PID)
    - Arrival time (when it enters the system)
    - Priority (0 = highest, 20 = lowest)
    - Operations (CPU, I/O, Resource Requests/Releases)
    """
    
    def __init__(self, pid, arrival, priority, operations):
        self.pid = pid
        self.arrival = arrival
        self.base_priority = priority      # Original priority (used for restoration)
        self.current_priority = priority   # Current priority (can be aged)
        self.operations = operations       # List of operations: CPU, I/O, REQ, REL
        
        # Process state tracking
        self.state = "NEW"                 # NEW, READY, RUNNING, WAITING, BLOCKED, TERMINATED, KILLED
        self.wait_time_in_ready = 0        # Time spent in Ready Queue (for aging)
        self.finish_time = 0               # When the process terminates
        
        # Resource management
        self.resources_held = {}           # Resources currently allocated to this process
        self.resources_requested = {}      # Resources this process is waiting for

    def __repr__(self):
        return f"P{self.pid}"


class OSScheduler:
    """
    Main scheduler simulation class.
    Manages the entire scheduling system including:
    - Process queues (NEW, READY, WAITING, BLOCKED, FINISHED)
    - CPU scheduling with priority and round robin
    - Resource allocation and deadlock detection
    - Time-based simulation
    """
    
    def __init__(self, system_resources):
        """Initialize the scheduler with available resources."""
        self.current_time = 0
        self.available_resources = system_resources.copy()  # Resources available in the system
        self.total_resources = system_resources.copy()      # Track original total for display
        
        # Process queues
        self.queue_new = []           # Processes not yet arrived
        self.queue_ready = []         # Processes ready to run
        self.queue_waiting = []       # Processes waiting for resources
        self.queue_io = []            # Processes performing I/O
        self.queue_finished = []      # Completed/terminated processes
        
        # Scheduling parameters
        self.time_quantum = 4         # Time slice for round robin (4 time units)
        self.current_quantum_used = 0 # Time used in current quantum
        
        # Execution log (for Gantt chart)
        self.execution_log = []       # Stores (PID, start_time, end_time) tuples
        
        # Deadlock detection log
        self.deadlock_log = []        # Records of detected deadlocks

    def add_process(self, process):
        """Add a new process to the NEW queue, sorted by arrival time."""
        self.queue_new.append(process)
        self.queue_new.sort(key=lambda p: p.arrival)

    def run_simulation(self):
        """
        Main simulation loop - executes until all processes are complete.
        At each time unit:
        1. Check for process arrivals
        2. Handle I/O completions
        3. Apply aging to ready processes
        4. Detect deadlocks (every 5 time units)
        5. Schedule and execute next process
        6. Increment time
        """
        print(f"[SYSTEM START] Available Resources: {self.available_resources}")
        print("\n" + "="*70)
        print("SIMULATION EXECUTION LOG")
        print("="*70 + "\n")

        # Continue until all queues are empty
        while (self.queue_new or self.queue_ready or self.queue_io or self.queue_waiting):
            
            # Step 1: Check for new process arrivals
            self._process_arrivals()
            
            # Step 2: Handle I/O completions
            self._handle_io_completions()
            
            # Step 3: Apply aging to processes waiting in Ready Queue
            self._apply_aging()
            
            # Step 4: Detect deadlocks every 5 time units if processes are waiting
            if self.queue_waiting and self.current_time > 0 and self.current_time % 5 == 0:
                self._detect_deadlock()
            
            # Step 5: Get next process and execute one time unit
            next_process = self._get_next_ready_process()
            if next_process:
                self._execute_process_step(next_process)
            
            # Step 6: Increment time
            self.current_time += 1
            
            # Safety check to prevent infinite loops (max 5000 time units)
            if self.current_time > 5000:
                print("\n[ERROR] Simulation timeout (exceeded 5000 time units)")
                break
        
        # Simulation complete - print results
        print("\n" + "="*70)
        print("SIMULATION EXECUTION LOG ENDED")
        print("="*70)
        print("\nGenerating Final Report...\n")
        
        self._print_gantt_chart()
        self._print_statistics()
        self._print_deadlock_summary()

    def _process_arrivals(self):
        """Check if any processes have arrived at the current time."""
        for process in self.queue_new[:]:  # Use slice to avoid list modification during iteration
            if process.arrival <= self.current_time:
                process.state = "READY"
                self.queue_ready.append(process)
                self.queue_new.remove(process)
                print(f"[Time {self.current_time:4d}] {process} arrives -> READY Queue")

    def _handle_io_completions(self):
        """Check if any processes have completed their I/O operations."""
        for process in self.queue_io[:]:
            if process.operations:
                current_op = process.operations[0]
                # Only process I/O operations
                if current_op['type'] == 'I/O':
                    current_op['duration'] -= 1
                    
                    # When I/O completes
                    if current_op['duration'] <= 0:
                        print(f"[Time {self.current_time:4d}] {process} I/O completed -> READY Queue")
                        process.operations.pop(0)
                        process.state = "READY"
                        self.queue_io.remove(process)
                        self.queue_ready.append(process)
                        process.wait_time_in_ready = 0  # Reset wait time after I/O

    def _apply_aging(self):
        """
        Apply aging to processes in the Ready Queue.
        Every 10 time units a process waits in Ready Queue, decrease priority by 1.
        NOTE: Priority value of 0 means highest priority. Aging DECREASES priority value.
        """
        for process in self.queue_ready:
            process.wait_time_in_ready += 1
            
            # Apply aging every 10 time units, but don't go below priority 0
            if process.wait_time_in_ready % 10 == 0 and process.current_priority > 0:
                process.current_priority -= 1
                print(f"[Time {self.current_time:4d}] Aging: {process} priority aged to {process.current_priority}")

    def _get_next_ready_process(self):
        """
        Select the next process to run based on scheduling criteria:
        1. Sort by CURRENT priority (lower value = higher priority)
        2. Tie-break by arrival time (earlier arrival = higher priority)
        Return None if Ready Queue is empty.
        """
        if not self.queue_ready:
            return None
        
        # Sort by: current priority (ascending), then arrival time (ascending)
        self.queue_ready.sort(key=lambda p: (p.current_priority, p.arrival))
        return self.queue_ready[0]

    def _execute_process_step(self, process):
        """
        Execute one step of the current process.
        A "step" can be:
        - Processing one unit of CPU burst
        - Requesting a resource
        - Releasing a resource
        - Starting an I/O operation
        """
        # If no operations left, process is complete
        if not process.operations:
            self._terminate_process(process)
            return

        current_op = process.operations[0]
        
        # ========== RESOURCE REQUEST ==========
        if current_op['type'] == 'REQ':
            resource_name = current_op['resource']
            amount_needed = current_op['amount']
            
            # Check if resource is available
            if self.available_resources.get(resource_name, 0) >= amount_needed:
                # Grant the resource
                self.available_resources[resource_name] -= amount_needed
                process.resources_held[resource_name] = process.resources_held.get(resource_name, 0) + amount_needed
                print(f"[Time {self.current_time:4d}] {process} GRANTED {resource_name}:{amount_needed}")
                
                # Remove this operation and continue with next
                process.operations.pop(0)
                self._execute_process_step(process)  # Recursively process next operation
            else:
                # Resource not available - process must wait
                print(f"[Time {self.current_time:4d}] {process} BLOCKED waiting for {resource_name}:{amount_needed}")
                process.state = "WAITING"
                process.resources_requested = {resource_name: amount_needed}
                self.queue_ready.remove(process)
                self.queue_waiting.append(process)
                self.current_quantum_used = 0  # Reset quantum when blocked

        # ========== RESOURCE RELEASE ==========
        elif current_op['type'] == 'REL':
            resource_name = current_op['resource']
            amount_to_release = current_op['amount']
            
            # Release the resource if process holds it
            if resource_name in process.resources_held:
                actual_release = min(process.resources_held[resource_name], amount_to_release)
                process.resources_held[resource_name] -= actual_release
                
                # Remove from tracking if all released
                if process.resources_held[resource_name] == 0:
                    del process.resources_held[resource_name]
                
                self.available_resources[resource_name] += actual_release
                print(f"[Time {self.current_time:4d}] {process} RELEASED {resource_name}:{actual_release}")
                
                # Check if any waiting processes can now proceed
                self._check_waiting_processes()
            
            # Remove this operation and continue
            process.operations.pop(0)
            self._execute_process_step(process)  # Recursively process next operation

        # ========== CPU BURST ==========
        elif current_op['type'] == 'CPU':
            # Execute one unit of CPU time
            current_op['duration'] -= 1
            self.current_quantum_used += 1
            process.state = "RUNNING"
            process.wait_time_in_ready = 0  # Reset wait time when running
            
            # Log this CPU unit for Gantt chart
            self.execution_log.append((process.pid, self.current_time, self.current_time + 1))
            
            # Check if CPU burst is complete
            if current_op['duration'] <= 0:
                print(f"[Time {self.current_time:4d}] {process} CPU burst complete")
                process.operations.pop(0)
                self.current_quantum_used = 0
            
            # Check if time quantum has expired (Round Robin)
            elif self.current_quantum_used >= self.time_quantum:
                print(f"[Time {self.current_time:4d}] {process} quantum expired -> preempted")
                self.current_quantum_used = 0
                
                # Move to end of Ready Queue (preemption)
                self.queue_ready.remove(process)
                self.queue_ready.append(process)
                process.state = "READY"

        # ========== I/O BURST ==========
        elif current_op['type'] == 'I/O':
            print(f"[Time {self.current_time:4d}] {process} starting I/O burst")
            process.state = "BLOCKED"
            self.queue_ready.remove(process)
            self.queue_io.append(process)
            self.current_quantum_used = 0  # Reset quantum when going to I/O

    def _check_waiting_processes(self):
        """
        After a resource is released, check if any waiting processes can now proceed.
        Move them back to the Ready Queue if their requested resources are available.
        """
        for process in self.queue_waiting[:]:
            can_proceed = True
            
            # Check if ALL requested resources are available
            for resource, amount in process.resources_requested.items():
                if self.available_resources.get(resource, 0) < amount:
                    can_proceed = False
                    break
            
            if can_proceed:
                # Process can proceed - restore ORIGINAL priority (important!)
                process.current_priority = process.base_priority
                process.wait_time_in_ready = 0
                process.state = "READY"
                process.resources_requested = {}
                self.queue_waiting.remove(process)
                
                # Add to front of Ready Queue (was blocked, now ready)
                self.queue_ready.insert(0, process)
                print(f"[Time {self.current_time:4d}] {process} unblocked -> READY Queue (priority restored to {process.base_priority})")

    def _terminate_process(self, process):
        """
        Terminate a process:
        1. Release all its held resources
        2. Check if any waiting processes can now proceed
        3. Record finish time
        """
        print(f"[Time {self.current_time:4d}] {process} TERMINATED")
        
        # Release all held resources
        for resource, amount in process.resources_held.items():
            self.available_resources[resource] += amount
        
        # Check if waiting processes can now proceed
        self._check_waiting_processes()
        
        # Update process state and add to finished queue
        process.state = "TERMINATED"
        process.finish_time = self.current_time
        self.queue_finished.append(process)
        
        # Remove from Ready Queue if still there
        if process in self.queue_ready:
            self.queue_ready.remove(process)

    def _detect_deadlock(self):
        """
        Deadlock Detection using Banker's Algorithm approach.
        Check if waiting processes form a circular wait condition.
        
        Algorithm:
        1. Build work vector from available resources
        2. For each waiting process, check if it can be satisfied
        3. If satisfied, mark finished and release its resources
        4. Repeat until no more progress
        5. Any process not marked as finished is deadlocked
        """
        if not self.queue_waiting:
            return
        
        # Step 1: Create work vector with available resources
        work = self.available_resources.copy()
        
        # Add resources held by non-waiting processes (Ready, I/O, New)
        for process in self.queue_ready + self.queue_io + self.queue_new:
            for resource, amount in process.resources_held.items():
                work[resource] = work.get(resource, 0) + amount
        
        # Step 2: Initialize finish state for waiting processes
        finish_state = {process.pid: False for process in self.queue_waiting}
        
        # Step 3: Safety check loop
        while True:
            progress_made = False
            
            for process in self.queue_waiting:
                # Skip if already finished in this iteration
                if finish_state[process.pid]:
                    continue
                
                # Check if process's request can be satisfied
                can_satisfy = True
                for resource, amount in process.resources_requested.items():
                    if amount > work.get(resource, 0):
                        can_satisfy = False
                        break
                
                # If we can satisfy this process
                if can_satisfy:
                    # Simulate granting and completion
                    for resource, amount in process.resources_held.items():
                        work[resource] = work.get(resource, 0) + amount
                    
                    finish_state[process.pid] = True
                    progress_made = True
            
            # If no progress made, we're done
            if not progress_made:
                break
        
        # Step 4: Check for deadlocked processes (not finished)
        deadlocked_processes = [p for p in self.queue_waiting if not finish_state[p.pid]]
        
        if deadlocked_processes:
            self.deadlock_log.append({
                'time': self.current_time,
                'processes': deadlocked_processes.copy()
            })
            print(f"\n[DEADLOCK DETECTED at Time {self.current_time}] Processes: {deadlocked_processes}")
            self._recover_from_deadlock(deadlocked_processes)

    def _recover_from_deadlock(self, deadlocked_processes):
        """
        Deadlock Recovery Strategy: Terminate the first deadlocked process (victim).
        This is a simple recovery method. Other strategies include:
        - Resource preemption
        - Rollback to earlier state
        """
        victim = deadlocked_processes[0]
        print(f"[RECOVERY] Terminating {victim} as deadlock victim\n")
        
        # Release all resources held by victim
        for resource, amount in victim.resources_held.items():
            self.available_resources[resource] += amount
        
        # Remove from waiting queue
        self.queue_waiting.remove(victim)
        
        # Mark as killed and add to finished queue
        victim.state = "KILLED"
        victim.finish_time = self.current_time
        self.queue_finished.append(victim)
        
        # Check if this allows other waiting processes to proceed
        self._check_waiting_processes()

    def _print_gantt_chart(self):
        """
        Print a visual Gantt chart showing process execution timeline.
        Merges consecutive time slots for the same process.
        """
        print("\n" + "="*70)
        print("GANTT CHART (Visual Timeline)")
        print("="*70)
        
        if not self.execution_log:
            print("No execution log available.")
            return
        
        # Step 1: Merge consecutive entries for the same process
        self.execution_log.sort(key=lambda x: x[1])  # Sort by start time
        
        merged_log = []
        if self.execution_log:
            current_pid, current_start, current_end = self.execution_log[0]
            
            for i in range(1, len(self.execution_log)):
                next_pid, next_start, next_end = self.execution_log[i]
                
                # Check if consecutive and same process
                if next_pid == current_pid and next_start == current_end:
                    current_end = next_end
                else:
                    merged_log.append((current_pid, current_start, current_end))
                    current_pid, current_start, current_end = next_pid, next_start, next_end
            
            # Add the last segment
            merged_log.append((current_pid, current_start, current_end))
        
        # Step 2: Print the visual chart
        print("|", end="")
        for pid, start, end in merged_log:
            print(f" P{pid:<2} |", end="")
        print()
        
        # Step 3: Print time scale below chart
        print("0", end="")
        for pid, start, end in merged_log:
            segment_width = len(f" P{pid} |")
            print(" " * (segment_width - 1) + f"{end}", end="")
        print("\n")

    def _print_statistics(self):
        """
        Print scheduling statistics:
        - Turnaround Time (finish_time - arrival_time)
        - CPU Time (actual time spent running)
        - Waiting Time (turnaround - CPU time)
        - Average Turnaround Time
        - Average Waiting Time
        """
        print("\n" + "="*70)
        print("SCHEDULING STATISTICS")
        print("="*70)
        
        # Sort by PID for display
        self.queue_finished.sort(key=lambda p: p.pid)
        
        # Print header
        print(f"{'PID':<5} {'Arrival':<8} {'Finish':<8} {'TAT':<8} {'CPU':<8} {'Wait':<8} {'Status':<12}")
        print("-" * 70)
        
        total_tat = 0
        total_wait = 0
        completed_count = 0
        
        for process in self.queue_finished:
            # Handle killed processes
            if process.state == "KILLED":
                print(f"P{process.pid:<4} {process.arrival:<8} {process.finish_time:<8} {'KILLED':<22}")
                continue
            
            # Calculate metrics
            turnaround_time = process.finish_time - process.arrival
            cpu_time = sum(end - start for pid, start, end in self.execution_log if pid == process.pid)
            wait_time = turnaround_time - cpu_time
            
            # Accumulate for averages
            total_tat += turnaround_time
            total_wait += wait_time
            completed_count += 1
            
            # Print process statistics
            print(f"P{process.pid:<4} {process.arrival:<8} {process.finish_time:<8} {turnaround_time:<8} {cpu_time:<8} {wait_time:<8} {'COMPLETED':<12}")
        
        # Print averages
        print("-" * 70)
        if completed_count > 0:
            avg_tat = total_tat / completed_count
            avg_wait = total_wait / completed_count
            print(f"{'AVERAGE':<5} {'':<8} {'':<8} {avg_tat:<8.2f} {'':<8} {avg_wait:<8.2f}")
        
        print()

    def _print_deadlock_summary(self):
        """Print summary of all detected deadlocks and recoveries."""
        if not self.deadlock_log:
            print("\n[INFO] No deadlocks detected during simulation.\n")
            return
        
        print("="*70)
        print("DEADLOCK DETECTION SUMMARY")
        print("="*70)
        
        for entry in self.deadlock_log:
            time = entry['time']
            processes = entry['processes']
            print(f"\nDeadlock detected at Time {time}:")
            print(f"  Involved processes: {processes}")
            print(f"  Recovery action: Terminated first process as victim")
        
        print()


# ==========================================
#  INPUT PARSING
# ==========================================

def parse_input(filename):
    """
    Parse input file and create simulation.
    
    Format:
    Line 1: [ResourceName,Count], [ResourceName,Count], ...
    Lines 2+: PID ArrivalTime Priority CPU{...} IO{...} CPU{...}
    
    Within CPU bursts:
    - R[ResourceName,Count]: Request resource
    - F[ResourceName,Count]: Release (Free) resource
    - Number: Execute for that many time units
    """
    try:
        with open(filename, 'r') as f:
            lines = f.readlines()
        
        if not lines:
            print("Error: Empty input file")
            return None
        
        # ===== Parse Resources (Line 1) =====
        resource_line = lines[0].strip()
        resources = {}
        
        # Try format: [R1,5], [R2,3]
        resource_matches = re.findall(r'\[(\w+),\s*(\d+)\]', resource_line)
        if resource_matches:
            resources = {name: int(count) for name, count in resource_matches}
        
        # Alternative format: R1:5, R2:3
        if not resources:
            resource_matches = re.findall(r'(\w+):\s*(\d+)', resource_line)
            resources = {name: int(count) for name, count in resource_matches}
        
        if not resources:
            print("Error: Could not parse resources from first line")
            return None
        
        print(f"[INPUT] Parsed resources: {resources}\n")
        
        # Create simulation with parsed resources
        sim = OSScheduler(resources)
        
        # ===== Parse Processes (Lines 2+) =====
        for line_num, line in enumerate(lines[1:], start=2):
            parts = line.strip().split()
            if len(parts) < 4:
                continue
            
            # Extract PID, Arrival, Priority
            try:
                pid = int(parts[0])
                arrival = int(parts[1])
                priority = int(parts[2])
            except ValueError:
                print(f"[WARNING] Line {line_num}: Could not parse PID/Arrival/Priority")
                continue
            
            # Extract burst string (everything from position 3 onwards)
            burst_string = line.split(maxsplit=3)[3]
            
            # Parse operations
            operations = _parse_operations(burst_string)
            if operations:
                process = Process(pid, arrival, priority, operations)
                sim.add_process(process)
                print(f"[INPUT] Process {pid}: Arrival={arrival}, Priority={priority}, Operations={len(operations)}")
        
        print()
        return sim
        
    except FileNotFoundError:
        print(f"Error: Input file '{filename}' not found")
        return None
    except Exception as e:
        print(f"Error parsing input: {e}")
        return None


def _parse_operations(burst_string):
    """
    Parse operation sequence from burst string.
    Examples:
    - CPU{5}
    - CPU{R[R1,2], 10, F[R1,2], 5}
    - IO{20}
    - CPU{5} IO{10} CPU{3}
    """
    operations = []
    
    # Find all CPU and I/O blocks
    # Pattern: CPU{...} or IO{...} or I/O{...}
    blocks = re.finditer(r'(CPU|I/O|IO)\s*\{([^}]*)\}', burst_string)
    
    for block_match in blocks:
        burst_type = block_match.group(1).upper()
        content = block_match.group(2).strip()
        
        if burst_type in ['I/O', 'IO']:
            # I/O burst: just a number
            duration = int(re.search(r'\d+', content).group())
            operations.append({
                'type': 'I/O',
                'duration': duration
            })
        
        else:  # CPU burst
            # Within CPU burst, can have: R[...], F[...], and numbers
            # Use regex to extract all tokens
            tokens = re.findall(r'(R\[\w+,\s*\d+\]|F\[\w+,\s*\d+\]|\d+)', content)
            
            for token in tokens:
                if token.startswith('R['):
                    # Request: R[ResourceName, Count]
                    match = re.search(r'R\[(\w+),\s*(\d+)\]', token)
                    if match:
                        operations.append({
                            'type': 'REQ',
                            'resource': match.group(1),
                            'amount': int(match.group(2))
                        })
                
                elif token.startswith('F['):
                    # Release: F[ResourceName, Count]
                    match = re.search(r'F\[(\w+),\s*(\d+)\]', token)
                    if match:
                        operations.append({
                            'type': 'REL',
                            'resource': match.group(1),
                            'amount': int(match.group(2))
                        })
                
                else:
                    # CPU execution time
                    operations.append({
                        'type': 'CPU',
                        'duration': int(token)
                    })
    
    return operations


# ==========================================
#  MAIN PROGRAM
# ==========================================

if __name__ == "__main__":
    """
    Main entry point of the program.
    Usage: python script.py [input_file]
    Default input file: input.txt
    """
    
    # Determine input file
    input_file = "input.txt"
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    print(f"Loading input file: {input_file}")
    print()
    
    # Parse input and run simulation
    simulation = parse_input(input_file)
    if simulation:
        simulation.run_simulation()
    else:
        print("Failed to initialize simulation")
