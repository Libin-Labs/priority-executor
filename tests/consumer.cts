import { createExecutor } from '@lp-labs/priority-executor';
const closed: boolean = createExecutor().closed;
void closed;
